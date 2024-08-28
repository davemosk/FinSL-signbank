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

parser = argparse.ArgumentParser()
parser.add_argument("--cached",
                    default=False,
                    required=False,
                    action="store_true",
                    help="Use keys generated on a previous non-cache run")
args = parser.parse_args()

# Setup
HEROKU = "/usr/bin/heroku"
AWS = "/usr/local/bin/aws"

RUN_MODE = "production"
if RUN_MODE == "production":
    print("PRODUCTION")
    NZSL_APP = "nzsl-signbank-production"
    AWS_S3_BUCKET = "nzsl-signbank-media-production"
else:
    print("STAGING")
    NZSL_APP = "nzsl-signbank-uat"
    AWS_S3_BUCKET = "nzsl-signbank-media-uat"

new_env = os.environ.copy()
new_env["AWS_PROFILE"] = "nzsl"

TMPDIR = "/tmp/nzsl"
try:
    os.makedirs(TMPDIR, exist_ok=True)
except OSError as err:
    print(f"Error creating directory: {err}")
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
    print("Using the video keys we recorded on the last non-cached run")
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
    for p in (
            NZSL_RAW_KEYS_FILE,
            NZSL_COOKED_KEYS_FILE,
            S3_BUCKET_RAW_KEYS_FILE,
            S3_BUCKET_ERROR_KEYS_FILE,
            S3_BUCKET_CONTENTS_FILE,
            S3_KEYS_NOT_IN_NZSL
    ):
        f = open(p, "a")
        f.truncate()
        f.close()

    # Get all keys from S3
    print(f"Getting raw S3 keys recursively ({AWS_S3_BUCKET}) ...")
    with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
        result = subprocess.run([AWS, "s3", "ls", f"s3://{AWS_S3_BUCKET}", "--recursive"],
                                env=new_env, shell=False, check=True,
                                text=True, stdout=f_obj)

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
        result = subprocess.run([HEROKU, "pg:psql", "DATABASE_URL", "--app", f"{NZSL_APP}",
                                 "-c", "select videofile, is_public from video_glossvideo"],
                                env=new_env, shell=False, check=True,
                                text=True, stdout=f_obj)

    # Remove the first 2 and last 2 lines, as we cannot control pg:psql's output formatting
    with open(NZSL_RAW_KEYS_FILE, "r") as f_obj:
        nzsl_raw_keys_list = f_obj.readlines()
        nzsl_raw_keys_list = nzsl_raw_keys_list[2:]
        nzsl_raw_keys_list = nzsl_raw_keys_list[:-2]
    print(f"{len(nzsl_raw_keys_list)} rows retrieved: {NZSL_RAW_KEYS_FILE}")

    # Put the raw lines back into the text file
    with open(NZSL_RAW_KEYS_FILE, "w") as f_obj:
        f_obj.writelines(nzsl_raw_keys_list)

    # Separate out the NZSL key columns
    # Write them to a dictionary so we can do fast operations on them
    for rawl in nzsl_raw_keys_list:
        columns = rawl.split("|")
        video_key = columns[0].strip()
        is_public = columns[1].strip().lower() == 't'
        nzsl_raw_keys_dict[video_key] = is_public
    # for item in nzsl_raw_keys_dict.items():
    #    print(item)

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
        [AWS, "s3api", "get-object-acl", "--output", "text", "--bucket", AWS_S3_BUCKET, "--key", video_key],
        env=new_env, shell=False, check=True,
        capture_output=True, text=True)
    print(result.stdout)
