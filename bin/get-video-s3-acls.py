#!/usr/bin/env python3
# Permissions required:
#  psql - access to heroku app's postgres
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
AWSCLIENT = "/usr/local/bin/aws"
PGCLIENT = "/usr/bin/psql"

# NZSL: Is there a database url defined in the environment?
DATABASE_URL = os.getenv("DATABASE_URL", None)

parser = argparse.ArgumentParser(
    description="You must have a configured AWSCLIENT profile to use this app. See the --awsprofile "
    "argument."
)
# Positional arguments
if DATABASE_URL:
    print("DATABASE_URL defined in environment")
else:
    parser.add_argument(
        "dburl",
        help=f"(REQUIRED) Database url (Overridden by DATABASE_URL environment variable)",
    )
# Optional arguments
parser.add_argument(
    "--awsprofile",
    default="nzsl",
    required=False,
    help=f"AWS configured profile to use (default: '%(default)s')",
)
parser.add_argument(
    "--production",
    default=False,
    required=False,
    action="store_true",
    help="Run in PRODUCTION mode, instead of STAGING (default: %(default)s)",
)
parser.add_argument(
    "--cached",
    default=False,
    required=False,
    action="store_true",
    help="Use keys generated on a previous non-cached run (default: %(default)s) "
    "(Don't mix PRODUCTION and STAGING!)",
)
parser.add_argument(
    "--pgclient",
    default=PGCLIENT,
    required=False,
    help=f"Postgres client path (default: %(default)s)",
)
parser.add_argument(
    "--awsclient",
    default=AWSCLIENT,
    required=False,
    help=f"AWS client path (default: %(default)s)",
)
args = parser.parse_args()

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

AWSCLIENT = args.awsclient
PGCLIENT = args.pgclient

if not DATABASE_URL:
    DATABASE_URL = args.dburl

if args.cached:
    print("Using the video keys we recorded on the last non-cached run.")
else:
    print("Generating keys from scratch.")

print(f"Mode:        {MODE_STR}")
print(f"  NZSL app:  {NZSL_APP}")
print(f"  S3 bucket: {AWS_S3_BUCKET}")
print(f"AWS profile: {new_env['AWS_PROFILE']}")
print(f"AWSCLIENT:   {AWSCLIENT}")
print(f"PGCLIENT:    {PGCLIENT}")
print(f"DATABASE_URL:\n{DATABASE_URL}")

TMPDIR = "/tmp/nzsl"
try:
    os.makedirs(TMPDIR, exist_ok=True)
except OSError as err:
    print(f"Error creating temporary directory: {TMPDIR} {err}")
    exit()
NZSL_RAW_KEYS_FILE = f"{TMPDIR}/nzsl_raw_keys.txt"
NZSL_COOKED_KEYS_FILE = f"{TMPDIR}/nzsl_cooked_keys.txt"
CSV_DELIMITER = ", "
S3_BUCKET_RAW_KEYS_FILE = f"{TMPDIR}/s3_bucket_raw_keys.txt"
S3_BUCKET_ERROR_KEYS_FILE = f"{TMPDIR}/s3_bucket_error_keys.csv"
S3_BUCKET_CONTENTS_FILE = f"{TMPDIR}/s3_bucket_contents.csv"
S3_KEYS_NOT_IN_NZSL_FILE = f"{TMPDIR}/s3_keys_not_in_nzsl.csv"
ALL_KEYS_FILE = f"{TMPDIR}/all_keys.csv"

nzsl_raw_keys_dict = {}
nzsl_cooked_keys_dict = {}
s3_keys_not_in_nzsl_list = []

# TODO This will replace everything
all_keys_dict = {}

if args.cached:
    print("NOT READY!")
    exit()
    # Pull all info from existing files
    try:
        with open(NZSL_COOKED_KEYS_FILE, "r") as f_obj:
            for line in f_obj.readlines():
                video_key, is_public = line.strip().split(CSV_DELIMITER)
                nzsl_cooked_keys_dict[video_key] = is_public
    except FileNotFoundError:
        print(f"File not found: {NZSL_COOKED_KEYS_FILE}")
        exit()
    try:
        with open(S3_KEYS_NOT_IN_NZSL_FILE, "r") as f_obj:
            s3_keys_not_in_nzsl_list = [line.strip() for line in f_obj.readlines()]
    except FileNotFoundError:
        print(f"File not found: {S3_KEYS_NOT_IN_NZSL_FILE}")
        exit()
    print(f"PRESENT: {len(nzsl_cooked_keys_dict)} keys")
    print(f"ABSENT:  {len(s3_keys_not_in_nzsl_list)} keys")
else:
    # Zero-out files
    for p in (
        NZSL_RAW_KEYS_FILE,
        NZSL_COOKED_KEYS_FILE,
        S3_BUCKET_RAW_KEYS_FILE,
        S3_BUCKET_ERROR_KEYS_FILE,
        S3_BUCKET_CONTENTS_FILE,
        S3_KEYS_NOT_IN_NZSL_FILE,
        ALL_KEYS_FILE
    ):
        f = open(p, "a")
        f.truncate()
        f.close()

    # Get all keys from AWS S3
    print(f"Getting raw AWS S3 keys recursively ({AWS_S3_BUCKET}) ...")
    with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
        result = subprocess.run(
            [AWSCLIENT, "s3", "ls", f"s3://{AWS_S3_BUCKET}", "--recursive"],
            env=new_env,
            shell=False,
            check=True,
            text=True,
            stdout=f_obj,
        )

    # Separate out just the key (also strip newline) from date, time, size, key
    # Put the keys in an in-memory list
    with open(S3_BUCKET_RAW_KEYS_FILE, "r") as f_obj:
        s3_bucket_raw_keys_list = [line.split()[3] for line in f_obj]
    print(f"{len(s3_bucket_raw_keys_list)} rows retrieved: {S3_BUCKET_RAW_KEYS_FILE}")

    # Write the keys back to the file, for cleanliness
    with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
        for line in s3_bucket_raw_keys_list:
            f_obj.write(f"{line}\n")

    # Get the video files info from NZSL Signbank
    print(f"Getting raw list of video file info from NZSL Signbank ({NZSL_APP}) ...")
    with open(NZSL_RAW_KEYS_FILE, "w") as f_obj:
        result = subprocess.run(
            [
                PGCLIENT,
                "-t",
                "-c",
                "select id as db_id, gloss_id, is_public, videofile from video_glossvideo",
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
    # Write them to a dictionary, so we can do fast operations on them
    for rawl in nzsl_raw_keys_list:
        rawl = rawl.strip()
        if not rawl:
            continue
        columns = rawl.split("|")
        db_id = columns[0].strip()
        gloss_id = columns[1].strip()
        is_public = columns[2].strip().lower() == "t"
        # 'videofile' data is also the key for S3
        video_key = columns[3].strip()
        # Each dictionary entry is all of these values
        nzsl_raw_keys_dict[video_key] = [db_id, gloss_id, is_public]

    # Get the s3 keys present and absent from our NZSL keys
    print("Getting S3 keys present and absent from NZSL Signbank ...")
    nkeys_present = 0
    nkeys_absent = 0
    for video_key in s3_bucket_raw_keys_list:
        if video_key in nzsl_raw_keys_dict:
            nkeys_present += 1
            # Add 'Present' column to start
            all_keys_dict[video_key] = [True] + nzsl_raw_keys_dict[video_key]
            nzsl_cooked_keys_dict[video_key] = nzsl_raw_keys_dict[video_key]
        else:
            nkeys_absent += 1
            s3_keys_not_in_nzsl_list.append(video_key)
            # Add 'Present' (absent) column to start
            all_keys_dict[video_key] = [False, "", "", ""]
    print(f"PRESENT: {nkeys_present} keys")
    print(f"ABSENT: {nkeys_absent} keys")

    # Write the "cooked" (i.e. present) keys back to a file
    with open(NZSL_COOKED_KEYS_FILE, "w") as f_obj:
        for video_key, is_public in nzsl_cooked_keys_dict.items():
            f_obj.write(f"{video_key}{CSV_DELIMITER}{str(is_public)}\n")

    # Write the absent keys back to a file
    with open(S3_KEYS_NOT_IN_NZSL_FILE, "w") as f_obj:
        for video_key in s3_keys_not_in_nzsl_list:
            f_obj.write(f"{video_key}\n")

    # Write all keys back to a file
    with open(ALL_KEYS_FILE, "w") as f_obj:
        for video_key, item_list in all_keys_dict.items():
            outstr = f"{video_key}{CSV_DELIMITER}{CSV_DELIMITER.join(map(str, item_list))}\n"
            f_obj.write(outstr)

# From the keys present in NZSL, get all their ACL information
print(f"Getting ACLs for keys from S3 ({AWS_S3_BUCKET}) ...")
for video_key, [is_present, db_id, gloss_id, is_public] in all_keys_dict.items():
    if not is_present:
        continue

    print("HUMPHREY")
    print(video_key)

    result = subprocess.run(
        [
            AWSCLIENT,
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
    print(f"Key:    {video_key}")
    print(f"Public: {is_public}")
    print(result.stdout)
