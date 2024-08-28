#!/usr/bin/env python3
# Permissions required:
#  heroku cli - access to app
#  aws s3 - NZSL IAM access
#  s3:GetObjectAcl permissions or READ_ACP access to the object
#  https://docs.aws.amazon.com/cli/latest/reference/s3api/get-object-acl.html


import os
import subprocess
import argparse
from pprint import pprint

# TODO
# We are using external apps just for the moment.
# These will be removed for native libraries.
PGCLIENT = "/usr/bin/psql"
AWS = "/usr/local/bin/aws"

parser = argparse.ArgumentParser(
    epilog="You must have a configured AWS profile to use this app. See the --awsprofile "
    "argument."
)
parser.add_argument(
    "--dburl",
    required=True,
    help=f"(REQUIRED) Database url (e.g. value of DATABASE_URL on Heroku)",
)
parser.add_argument(
    "--cached",
    default=False,
    required=False,
    action="store_true",
    help="Use keys generated on a previous non-cache run (default: False)",
)
parser.add_argument(
    "--production",
    default=False,
    required=False,
    action="store_true",
    help="Run in PRODUCTION mode (instead of STAGING) (default: False/STAGING)",
)
parser.add_argument(
    "--pgclient",
    default=PGCLIENT,
    required=False,
    help=f"Postgres client path (default: {PGCLIENT})",
)
parser.add_argument(
    "--awsprofile",
    default="nzsl",
    required=False,
    help=f"AWS configured profile to use (default: 'nzsl')",
)
parser.add_argument(
    "--awsclient", default=AWS, required=False, help=f"AWS client path (default: {AWS})"
)
args = parser.parse_args()

DATABASE_URL = args.dburl
PGCLIENT = args.pgclient
AWS = args.awsclient

if args.production:
    MODE_STR = "PRODUCTION"
    NZSL_APP = "nzsl-signbank-production"
    AWS_S3_BUCKET = "nzsl-signbank-media-production"
else:
    MODE_STR = "STAGING"
    NZSL_APP = "nzsl-signbank-uat"
    AWS_S3_BUCKET = "nzsl-signbank-media-uat"

new_env = os.environ.copy()
new_env["AWS_PROFILE"] = args.awsprofile

print(f"Mode:                 {MODE_STR}")
print(f"Target NZSL app:      {NZSL_APP}")
print(f"Target AWS S3 bucket: {AWS_S3_BUCKET}")
print(f"AWS profile using:    {new_env['AWS_PROFILE']}")

TMPDIR = "/tmp/nzsl"
try:
    os.makedirs(TMPDIR, exist_ok=True)
except OSError as err:
    print(f"Error creating temporary directory: {TMPDIR} {err}")
    exit()
NZSL_RAW_KEYS_FILE = f"{TMPDIR}/nzsl_raw_keys.txt"
NZSL_COOKED_KEYS_FILE = f"{TMPDIR}/nzsl_cooked_keys.txt"
S3_BUCKET_RAW_KEYS_FILE = f"{TMPDIR}/s3_bucket_raw_keys.txt"
S3_BUCKET_ERROR_KEYS_FILE = f"{TMPDIR}/s3_bucket_error_keys.csv"
S3_BUCKET_CONTENTS_FILE = f"{TMPDIR}/s3_bucket_contents.csv"
S3_KEYS_NOT_IN_NZSL = f"{TMPDIR}/s3_keys_not_in_nzsl.csv"

nzsl_raw_keys_dict = {}
nzsl_cooked_keys_dict = {}
s3_keys_not_in_nzsl_list = []

if args.cached:
    print("Using the video keys we recorded on the last non-cached run.")
    try:
        with open(NZSL_COOKED_KEYS_FILE, "r") as f_obj:
            for line in f_obj.readlines():
                video_key, is_public = line.strip().split(", ")
                nzsl_cooked_keys_dict[video_key] = is_public
    except FileNotFoundError:
        print(f"File not found: {NZSL_COOKED_KEYS_FILE}")
        exit()
    print(f"PRESENT: {len(nzsl_cooked_keys_dict)} keys")
else:
    print("Generating keys from scratch.")
    for p in (
        NZSL_RAW_KEYS_FILE,
        NZSL_COOKED_KEYS_FILE,
        S3_BUCKET_RAW_KEYS_FILE,
        S3_BUCKET_ERROR_KEYS_FILE,
        S3_BUCKET_CONTENTS_FILE,
        S3_KEYS_NOT_IN_NZSL,
    ):
        f = open(p, "a")
        f.truncate()
        f.close()

    # Get all keys from S3
    print(f"Getting raw S3 keys recursively ({AWS_S3_BUCKET}) ...")
    with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
        result = subprocess.run(
            [AWS, "s3", "ls", f"s3://{AWS_S3_BUCKET}", "--recursive"],
            env=new_env,
            shell=False,
            check=True,
            text=True,
            stdout=f_obj,
        )

    # Separate out just the keys (also strips newlines)
    # Put them in an in-memory list
    with open(S3_BUCKET_RAW_KEYS_FILE, "r") as f_obj:
        s3_bucket_raw_keys_list = [line.split()[3] for line in f_obj]
    print(f"{len(s3_bucket_raw_keys_list)} rows retrieved: {S3_BUCKET_RAW_KEYS_FILE}")

    # Write the keys back to the file
    with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
        for line in s3_bucket_raw_keys_list:
            f_obj.write(f"{line}\n")

    # Get the video file keys from NZSL Signbank
    print(f"Getting raw video file keys from NZSL Signbank ({NZSL_APP}) ...")
    with open(NZSL_RAW_KEYS_FILE, "w") as f_obj:
        result = subprocess.run(
            [
                PGCLIENT,
                "-t",
                "-c",
                "select videofile, is_public from video_glossvideo",
                f"{DATABASE_URL}",
            ],
            env=new_env,
            shell=False,
            check=True,
            text=True,
            stdout=f_obj,
        )
    with open(NZSL_RAW_KEYS_FILE, "r") as f_obj:
        nzsl_raw_keys_list = f_obj.readlines()
    print(f"{len(nzsl_raw_keys_list)} rows retrieved: {NZSL_RAW_KEYS_FILE}")

    # Separate out the NZSL key columns
    # Write them to a dictionary so we can do fast operations on them
    for rawl in nzsl_raw_keys_list:
        rawl = rawl.strip()
        if not rawl:
            continue
        columns = rawl.split("|")
        video_key = columns[0].strip()
        is_public = columns[1].strip().lower() == "t"
        nzsl_raw_keys_dict[video_key] = is_public

    # Get the s3 keys present and absent from our NZSL keys
    print("Getting S3 keys present and absent from NZSL Signbank ...")
    for video_key in s3_bucket_raw_keys_list:
        if video_key in nzsl_raw_keys_dict:
            nzsl_cooked_keys_dict[video_key] = nzsl_raw_keys_dict[video_key]
        else:
            s3_keys_not_in_nzsl_list.append(video_key)
    print(f"PRESENT: {len(nzsl_cooked_keys_dict)} keys")
    print(f"ABSENT: {len(s3_keys_not_in_nzsl_list)} keys")
    # Write just the cooked keys back to a file
    # This is mainly for Debug
    with open(NZSL_COOKED_KEYS_FILE, "w") as f_obj:
        for video_key, is_public in nzsl_cooked_keys_dict.items():
            f_obj.write(f"{video_key}, {str(is_public)}\n")

# From the keys present in NZSL, get all their ACL information
print(f"Getting ACLs for keys from S3 ({AWS_S3_BUCKET}) ...")
for video_key, is_public in nzsl_cooked_keys_dict.items():
    video_key = video_key.strip()
    print(f"Key:    {video_key}")
    print(f"Public: {is_public}")
    result = subprocess.run(
        [
            AWS,
            "s3api",
            "get-object-acl",
            "--output",
            "text",
            "--bucket",
            AWS_S3_BUCKET,
            "--key",
            video_key,
        ],
        env=new_env,
        shell=False,
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
