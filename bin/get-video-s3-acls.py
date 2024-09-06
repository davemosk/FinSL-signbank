#!/usr/bin/env python3
# Permissions required:
#  psql - access to heroku app's postgres
#  aws s3 - NZSL IAM access
#  s3:GetObjectAcl permissions or READ_ACP access to the object
#  https://docs.aws.amazon.com/cli/latest/reference/s3api/get-object-acl.html

import os
import sys
import subprocess
import argparse
import json
import re

parser = argparse.ArgumentParser(
    description="You must setup: An AWS auth means, eg. AWS_PROFILE env var. "
    "Postgres access details, eg. DATABASE_URL env var."
)
parser.add_argument(
    "--mode",
    default="uat",
    required=False,
    help="Mode to run in, eg 'production, 'uat', etc (default: '%(default)s')",
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
    default="/usr/bin/psql",
    required=False,
    help=f"Postgres client path (default: %(default)s)",
)
parser.add_argument(
    "--awsclient",
    default="/usr/local/bin/aws",
    required=False,
    help=f"AWS client path (default: %(default)s)",
)
args = parser.parse_args()

# Globals
AWSCLIENT = args.awsclient
PGCLIENT = args.pgclient
DATABASE_URL = os.getenv("DATABASE_URL", "")
CSV_DELIMITER = ","
AWS_S3_BUCKET = f"nzsl-signbank-media-{args.mode}"
TMPDIR = "/tmp/nzsl"
try:
    os.makedirs(TMPDIR, exist_ok=True)
except OSError as err:
    print(f"Error creating temporary directory: {TMPDIR} {err}", file=sys.stderr)
    exit()
ALL_KEYS_CACHE_FILE = f"{TMPDIR}/all_keys_cache.csv"

# Vars
nzsl_raw_keys_dict = {}
s3_bucket_raw_keys_list = []
all_keys_dict = {}


# Truncate files, creating them if necessary
def init_files(files_list=(ALL_KEYS_CACHE_FILE,)):
    for p in files_list:
        f = open(p, "a")
        f.truncate()
        f.close()


# Pull all info from existing cache file
def get_keys_from_cache_file(cache_file=ALL_KEYS_CACHE_FILE):
    nkeys_present = 0
    nkeys_absent = 0
    this_all_keys_dict = {}
    with open(cache_file, "r") as f_obj:
        for line in f_obj.readlines():
            (
                video_key,
                is_present_str,
                db_id_str,
                gloss_id_str,
                is_public_str,
            ) = line.strip().split(CSV_DELIMITER)

            is_present = is_present_str.strip().lower() == "true"
            if is_present:
                nkeys_present += 1
                db_id = int(db_id_str)
                # Some don't have gloss_id's
                try:
                    gloss_id = int(gloss_id_str)
                except ValueError:
                    gloss_id = None
                is_public = is_public_str.strip().lower() == "true"
            else:
                nkeys_absent += 1
                db_id = None
                gloss_id = None
                is_public = None

            this_all_keys_dict[video_key] = [is_present, db_id, gloss_id, is_public]

        print(f"PRESENT: {nkeys_present} keys", file=sys.stderr)
        print(f"ABSENT:  {nkeys_absent} keys", file=sys.stderr)

        return this_all_keys_dict


# Get all keys from AWS S3
def get_s3_bucket_raw_keys_list(s3_bucket=AWS_S3_BUCKET):
    print(f"Getting raw AWS S3 keys recursively ({s3_bucket}) ...", file=sys.stderr)
    result = subprocess.run(
        [
            AWSCLIENT,
            "s3",
            "ls",
            f"s3://{s3_bucket}",
            "--recursive",
        ],
        env=os.environ,
        capture_output=True,
        check=True,
        text=True,
    )

    # Separate out just the key from date, time, size, key
    this_s3_bucket_raw_keys_list = []
    for line in result.stdout.split("\n"):
        if line:
            this_s3_bucket_raw_keys_list.append(re.split(r"\s+", line, 3)[3])

    print(
        f"{len(this_s3_bucket_raw_keys_list)} rows retrieved",
        file=sys.stderr,
    )

    return this_s3_bucket_raw_keys_list


# Get the video files info from NZSL Signbank
def get_nzsl_raw_keys_dict():
    this_nzsl_raw_keys_dict = {}
    print(
        f"Getting raw list of video file info from NZSL Signbank ...",
        file=sys.stderr,
    )
    result = subprocess.run(
        [
            PGCLIENT,
            "-c",
            "COPY (SELECT id AS db_id, gloss_id, is_public, videofile FROM video_glossvideo) "
            "TO STDOUT WITH (FORMAT CSV)",
            f"{DATABASE_URL}",
        ],
        env=os.environ,
        capture_output=True,
        check=True,
        text=True,
    )

    # Separate out the NZSL db columns
    # Write them to a dictionary, so we can do fast operations
    for rawl in result.stdout.split("\n"):
        rawl = rawl.strip()
        if not rawl:
            continue
        [db_id, gloss_id, is_public, video_key] = rawl.split(",")
        this_nzsl_raw_keys_dict[video_key] = [db_id, gloss_id, is_public.lower() == "t"]

    print(
        f"{len(this_nzsl_raw_keys_dict)} rows retrieved",
        file=sys.stderr,
    )

    return this_nzsl_raw_keys_dict


# Get the s3 keys present and absent from our NZSL keys
def create_all_keys_dict(this_s3_bucket_raw_keys_list, this_nzsl_raw_keys_dict):
    print("Getting S3 keys present and absent from NZSL Signbank ...", file=sys.stderr)
    nkeys_present = 0
    nkeys_absent = 0
    this_all_keys_dict = {}
    for video_key in this_s3_bucket_raw_keys_list:
        if video_key in this_nzsl_raw_keys_dict:
            nkeys_present += 1
            # Add 'Present' column to start
            this_all_keys_dict[video_key] = [True] + this_nzsl_raw_keys_dict[video_key]
        else:
            nkeys_absent += 1
            # Add 'Present' (absent) column to start
            this_all_keys_dict[video_key] = [False, "", "", ""]
    print(f"PRESENT: {nkeys_present} keys", file=sys.stderr)
    print(f"ABSENT:  {nkeys_absent} keys", file=sys.stderr)

    # Write all keys back to a cache file
    with open(ALL_KEYS_CACHE_FILE, "w") as f_obj:
        for video_key, item_list in this_all_keys_dict.items():
            outstr = (
                f"{video_key}{CSV_DELIMITER}{CSV_DELIMITER.join(map(str, item_list))}\n"
            )
            f_obj.write(outstr)

    return this_all_keys_dict


# From the keys present in NZSL, get all their ACL information
def output_csv(this_all_keys_dict):
    print(f"Getting ACLs for keys from S3 ({AWS_S3_BUCKET}) ...", file=sys.stderr)

    # CSV header
    csv_header_list = [
        "Video S3 Key",
        "Postgres ID",
        "Gloss ID",
        "Signbank Public",
        "Expected S3 Canned ACL",
        "Actual S3 Canned ACL",
    ]
    print(CSV_DELIMITER.join(csv_header_list))

    for video_key, [
        is_present,
        db_id,
        gloss_id,
        is_public,
    ] in this_all_keys_dict.items():

        # See signbank/video/models.py, line 59, in function set_public_acl()
        canned_acl_expected = "public-read" if is_public else "private"
        run_array = [
            AWSCLIENT,
            "s3api",
            "get-object-acl",
            "--output",
            "json",
            "--bucket",
            AWS_S3_BUCKET,
            "--key",
            video_key,
        ]
        result = subprocess.run(
            run_array,
            env=os.environ,
            shell=False,
            check=True,
            capture_output=True,
            text=True,
        )
        canned_acl = "unknown"
        acls_grants_json = json.loads(result.stdout)["Grants"]
        if len(acls_grants_json) > 1:
            if (
                acls_grants_json[0]["Permission"] == "FULL_CONTROL"
                and acls_grants_json[1]["Permission"] == "READ"
            ):
                canned_acl = "public-read"
        else:
            if acls_grants_json[0]["Permission"] == "FULL_CONTROL":
                canned_acl = "private"

        if not is_present:
            print(f"{video_key},,,,,{canned_acl}")
            continue

        # CSV columns
        csv_column_list = [
            f"{video_key}",
            f"{db_id}",
            f"{gloss_id}",
            f"{is_public}",
            f"{canned_acl_expected}",
            f"{canned_acl}",
        ]
        print(CSV_DELIMITER.join(csv_column_list))


print(f"Mode:        {args.mode}", file=sys.stderr)
print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
print(f"AWSCLIENT:   {AWSCLIENT}", file=sys.stderr)
print(f"PGCLIENT:    {PGCLIENT}", file=sys.stderr)
if "AWS_PROFILE" in os.environ:
    print(f"AWS profile: {os.environ['AWS_PROFILE']}", file=sys.stderr)

if args.cached:
    print(f"Using video keys from cache file ({ALL_KEYS_CACHE_FILE}).", file=sys.stderr)
    all_keys_dict = get_keys_from_cache_file()
else:
    print("Generating video keys from scratch.", file=sys.stderr)
    init_files()
    s3_bucket_raw_keys_list = get_s3_bucket_raw_keys_list()
    nzsl_raw_keys_dict = get_nzsl_raw_keys_dict()
    all_keys_dict = create_all_keys_dict(s3_bucket_raw_keys_list, nzsl_raw_keys_dict)

output_csv(all_keys_dict)
