#!/usr/bin/env python3
# Permissions required:
#  heroku cli - access to app
#  aws s3 - NZSL IAM access
#  s3:GetObjectAcl permissions or READ_ACP access to the object
#  https://docs.aws.amazon.com/cli/latest/reference/s3api/get-object-acl.html

import os
import subprocess


# Setup
# TODO See how difficult using native API calls would be.
HEROKU = "/usr/bin/heroku"
AWS = "/usr/local/bin/aws"

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


# Get all keys from S3
"""
print(f"Getting raw S3 keys recursively ({AWS_S3_BUCKET}) ...")
with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
    result = subprocess.run([AWS, "s3", "ls", f"s3://{AWS_S3_BUCKET}", "--recursive"],
                            env=new_env, shell=False, check=True,
                            text=True, stdout=f_obj)
num_lines = sum(1 for _ in open(S3_BUCKET_RAW_KEYS_FILE))
print(f"{num_lines} rows retrieved: {S3_BUCKET_RAW_KEYS_FILE}")
"""

# Get the video file keys from NZSL Signbank
print(f"Getting raw video file keys from NZSL Signbank ({NZSL_APP}) ...")
with open(NZSL_RAW_KEYS_FILE, "w") as f_obj:
    result = subprocess.run([HEROKU, "pg:psql", "DATABASE_URL", "--app", f"{NZSL_APP}",
                             "-c", "select videofile, is_public from video_glossvideo"],
                            env=new_env, shell=False, check=True,
                            text=True, stdout=f_obj)
# Remove the first 2 and last 2 lines, as we cannot control pg:psql
with open(NZSL_RAW_KEYS_FILE, "r") as f_obj:
    lines = f_obj.readlines()
    lines = lines[2:]
    lines = lines[:-2]
    for x in lines:
        print(x)

#num_lines = sum(1 for _ in open(NZSL_RAW_KEYS_FILE))
#print(f"{num_lines} rows retrieved: {NZSL_RAW_KEYS_FILE}")

