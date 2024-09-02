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

# TODO
# We are using external apps just for the moment.
# These will be removed for native libraries.
AWSCLIENT = "/usr/local/bin/aws"
PGCLIENT = "/usr/bin/psql"

# NZSL: Is there a database url defined in the environment?
DATABASE_URL = os.getenv("DATABASE_URL", None)
if not DATABASE_URL:
    print("You must define DATABASE_URL in the environment.", file=sys.stderr)
    exit()

# AWS: Is there an AWS_PROFILE defined in the environment?
AWS_PROFILE = os.getenv("AWS_PROFILE", None)
if not AWS_PROFILE:
    print(
        "You must define AWS_PROFILE in the environment. Eg. AWS_PROFILE='nzsl'",
        file=sys.stderr,
    )
    exit()

parser = argparse.ArgumentParser(
    description="You must define, in the environment: AWS_PROFILE, DATABASE_URL"
)

# Optional arguments
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

NZSL_APP = f"nzsl-signbank-{args.mode}"
AWS_S3_BUCKET = f"nzsl-signbank-media-{args.mode}"

# Get the environment
new_env = os.environ.copy()

AWSCLIENT = args.awsclient
PGCLIENT = args.pgclient

if args.cached:
    print(
        "Using the video keys we recorded on the last non-cached run.", file=sys.stderr
    )
else:
    print("Generating keys from scratch.", file=sys.stderr)

print(f"Mode:        {args.mode}", file=sys.stderr)
print(f"  NZSL app:  {NZSL_APP}", file=sys.stderr)
print(f"  S3 bucket: {AWS_S3_BUCKET}", file=sys.stderr)
print(f"AWS profile: {new_env['AWS_PROFILE']}", file=sys.stderr)
print(f"AWSCLIENT:   {AWSCLIENT}", file=sys.stderr)
print(f"PGCLIENT:    {PGCLIENT}", file=sys.stderr)
print(f"DATABASE_URL:\n{new_env['DATABASE_URL']}", file=sys.stderr)

TMPDIR = "/tmp/nzsl"
try:
    os.makedirs(TMPDIR, exist_ok=True)
except OSError as err:
    print(f"Error creating temporary directory: {TMPDIR} {err}", file=sys.stderr)
    exit()

CSV_DELIMITER = ","
NZSL_POSTGRES_RAW_KEYS_FILE = f"{TMPDIR}/nzsl_postgres_raw_keys.txt"
S3_BUCKET_RAW_KEYS_FILE = f"{TMPDIR}/s3_bucket_raw_keys.txt"
ALL_KEYS_FILE = f"{TMPDIR}/all_keys.csv"

nzsl_raw_keys_dict = {}
s3_bucket_raw_keys_list = []
all_keys_dict = {}

nkeys_present = 0
nkeys_absent = 0

if args.cached:
    # Pull all info from existing file
    try:
        with open(ALL_KEYS_FILE, "r") as f_obj:
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

                all_keys_dict[video_key] = [is_present, db_id, gloss_id, is_public]

        print(f"PRESENT: {nkeys_present} keys", file=sys.stderr)
        print(f"ABSENT:  {nkeys_absent} keys", file=sys.stderr)
    except FileNotFoundError:
        print(f"File not found: {ALL_KEYS_FILE}", file=sys.stderr)
        exit()
else:
    # Zero-out files
    for p in (NZSL_POSTGRES_RAW_KEYS_FILE, S3_BUCKET_RAW_KEYS_FILE, ALL_KEYS_FILE):
        f = open(p, "a")
        f.truncate()
        f.close()

    # Get all keys from AWS S3
    print(f"Getting raw AWS S3 keys recursively ({AWS_S3_BUCKET}) ...", file=sys.stderr)
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
    print(
        f"{len(s3_bucket_raw_keys_list)} rows retrieved: {S3_BUCKET_RAW_KEYS_FILE}",
        file=sys.stderr,
    )

    # Write the keys back to the file, for cleanliness
    with open(S3_BUCKET_RAW_KEYS_FILE, "w") as f_obj:
        for line in s3_bucket_raw_keys_list:
            f_obj.write(f"{line}\n")

    # Get the video files info from NZSL Signbank
    print(
        f"Getting raw list of video file info from NZSL Signbank ({NZSL_APP}) ...",
        file=sys.stderr,
    )
    with open(NZSL_POSTGRES_RAW_KEYS_FILE, "w") as f_obj:
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
    with open(NZSL_POSTGRES_RAW_KEYS_FILE, "r") as f_obj:
        nzsl_raw_keys_list = f_obj.readlines()
    print(
        f"{len(nzsl_raw_keys_list)} rows retrieved: {NZSL_POSTGRES_RAW_KEYS_FILE}",
        file=sys.stderr,
    )

    # Separate out the NZSL db columns
    # Write them to a dictionary, so we can do fast operations
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
        # Each dictionary slot contains these values
        nzsl_raw_keys_dict[video_key] = [db_id, gloss_id, is_public]

    # Get the s3 keys present and absent from our NZSL keys
    print("Getting S3 keys present and absent from NZSL Signbank ...", file=sys.stderr)
    for video_key in s3_bucket_raw_keys_list:
        if video_key in nzsl_raw_keys_dict:
            nkeys_present += 1
            # Add 'Present' column to start
            all_keys_dict[video_key] = [True] + nzsl_raw_keys_dict[video_key]
        else:
            nkeys_absent += 1
            # Add 'Present' (absent) column to start
            all_keys_dict[video_key] = [False, "", "", ""]
    print(f"PRESENT: {nkeys_present} keys", file=sys.stderr)
    print(f"ABSENT:  {nkeys_absent} keys", file=sys.stderr)

    # Write all keys back to a file
    with open(ALL_KEYS_FILE, "w") as f_obj:
        for video_key, item_list in all_keys_dict.items():
            outstr = (
                f"{video_key}{CSV_DELIMITER}{CSV_DELIMITER.join(map(str, item_list))}\n"
            )
            f_obj.write(outstr)

# From the keys present in NZSL, get all their ACL information
print(f"Getting ACLs for keys from S3 ({AWS_S3_BUCKET}) ...", file=sys.stderr)
# CSV header
print(
    f"Video S3 Key{CSV_DELIMITER}Present in Signbank?{CSV_DELIMITER}Postgres ID{CSV_DELIMITER}Gloss ID{CSV_DELIMITER}"
    f"Signbank is_public?{CSV_DELIMITER}Expected S3 Canned ACL{CSV_DELIMITER}Actual S3 Canned ACL"
    f"{CSV_DELIMITER}Correct Canned ACL?{CSV_DELIMITER}Raw ACL data"
)
for video_key, [is_present, db_id, gloss_id, is_public] in all_keys_dict.items():
    canned_acl = ""
    canned_acl_expected = ""
    raw_acl = ""
    if is_present:
        # See signbank/video/models.py, line 59, in function set_public_acl()
        canned_acl_expected = "public-read" if is_public else "private"
        result = subprocess.run(
            [
                AWSCLIENT,
                "s3api",
                "get-object-acl",
                "--output",
                "json",
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
        raw_acl = result.stdout.replace("\n", " ")
        acls_grants_json = json.loads(result.stdout)["Grants"]
        if len(acls_grants_json) > 1:
            if (
                acls_grants_json[0]["Permission"] == "FULL_CONTROL"
                and acls_grants_json[1]["Permission"] == "READ"
            ):
                canned_acl = "public-read"
            else:
                canned_acl = "Unknown ACL"
        else:
            if acls_grants_json[0]["Permission"] == "FULL_CONTROL":
                canned_acl = "private"
            else:
                canned_acl = "Unknown ACL"

    # CSV columns
    print(f"{video_key}", end=CSV_DELIMITER)
    print(f"{is_present}", end=CSV_DELIMITER)
    print(f"{db_id if is_present else ''}", end=CSV_DELIMITER)
    print(f"{gloss_id if is_present else ''}", end=CSV_DELIMITER)
    print(f"{is_public if is_present else ''}", end=CSV_DELIMITER)
    print(f"{canned_acl_expected}", end=CSV_DELIMITER)
    print(f"{canned_acl}", end=CSV_DELIMITER)
    print(f"{str(canned_acl_expected == canned_acl) if is_present else ''}", end=CSV_DELIMITER)
    print(f"{raw_acl}")
