#!/usr/bin/env -S python3 -u
# Bang line above passes '-u' to python, for unbuffered output
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
    "--env",
    default="uat",
    required=False,
    help="Environment to run against, eg 'production, 'uat', etc (default: '%(default)s')",
)
parser.add_argument(
    "--cached",
    default=False,
    required=False,
    action="store_true",
    help="Use video keys generated on a previous non-cached run (default: %(default)s) "
    "(Do not mix production and staging!)",
)
parser.add_argument(
    "--pgcli",
    default="/usr/bin/psql",
    required=False,
    help=f"Postgres client path (default: %(default)s)",
)
parser.add_argument(
    "--awscli",
    default="/usr/local/bin/aws",
    required=False,
    help=f"AWS client path (default: %(default)s)",
)
parser.add_argument(
    "--tmpdir",
    default="/tmp/nzsl",
    required=False,
    help=f"Temp dir path (default: %(default)s)",
)
args = parser.parse_args()

# Globals
CSV_DELIMITER = ","
DATABASE_URL = os.getenv("DATABASE_URL", "")
AWSCLI = args.awscli
PGCLI = args.pgcli
AWS_S3_BUCKET = f"nzsl-signbank-media-{args.env}"
TMPDIR = args.tmpdir
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


# DICTIONARY and CACHE FILE format
# This is used at several points
# Essentially video_key + in_nzsl + in_s3 + nzsl_raw_keys_dict
#   video_key(str) ->
#       in_nzsl(bool),
#       in_s3(bool),
#       gloss_id(int),
#       gloss_idgloss(str),
#       created_at(str),
#       gloss_public(bool),
#       video_public(bool)
#       video_id(int)
# TODO For cache file format maybe move the video key to the end of the row, for consistency


# Pull all info from existing cache file
def get_keys_from_cache_file():
    this_all_keys_dict = {}
    with open(ALL_KEYS_CACHE_FILE, "r") as cache_file:
        for line in cache_file.readlines():
            (
                video_key,
                key_in_nzsl_str,
                key_in_s3_str,
                gloss_id_str,
                gloss_idgloss,
                created_at,
                gloss_public_str,
                video_public_str,
                video_id_str,
            ) = line.strip().split(CSV_DELIMITER)

            key_in_nzsl = key_in_nzsl_str.strip().lower() == "true"
            key_in_s3 = key_in_s3_str.strip().lower() == "true"
            if key_in_nzsl:
                video_id = int(video_id_str)
                # Some have no gloss_id
                try:
                    gloss_id = int(gloss_id_str)
                except ValueError:
                    gloss_id = None
                gloss_public = gloss_public_str.strip().lower() == "true"
                video_public = video_public_str.strip().lower() == "true"
            else:
                video_id = ""
                gloss_id = ""
                gloss_public = ""
                video_public = ""

            this_all_keys_dict[video_key] = [
                key_in_nzsl,
                key_in_s3,
                video_id,
                gloss_id,
                gloss_public,
                video_public,
            ]

        return this_all_keys_dict


# Get all keys from AWS S3
def get_s3_bucket_raw_keys_list(s3_bucket=AWS_S3_BUCKET):
    print(f"Getting raw AWS S3 keys recursively ({s3_bucket}) ...", file=sys.stderr)
    result = subprocess.run(
        [
            AWSCLI,
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
    print(
        f"Getting raw list of video file info from NZSL Signbank ...",
        file=sys.stderr,
    )
    this_nzsl_raw_keys_dict = {}
    # Column renaming is purely for readability
    # Special delimiter because columns might contain commas
    result = subprocess.run(
        [
            PGCLI,
            "-c",
            "COPY ("
            "SELECT "
            "dg.id AS gloss_id, "
            "dg.idgloss AS gloss_idgloss, "
            "dg.created_at, "
            "dg.published AS gloss_public, "
            "vg.is_public AS video_public, "
            "vg.id AS video_id, "
            "vg.videofile AS video_key "
            "FROM dictionary_gloss AS dg JOIN video_glossvideo AS vg ON vg.gloss_id = dg.id"
            ") TO STDOUT WITH DELIMITER AS '|'",
            f"{DATABASE_URL}",
        ],
        env=os.environ,
        capture_output=True,
        check=True,
        text=True,
    )

    # Separate the NZSL db columns
    # Write them to a dictionary, so we can do fast operations
    for rawl in result.stdout.split("\n"):
        rawl = rawl.strip()
        if not rawl:
            continue
        [
            gloss_id,
            gloss_idgloss,
            created_at,
            gloss_public,
            video_public,
            video_id,
            video_key,
        ] = rawl.split("|")
        this_nzsl_raw_keys_dict[video_key] = [
            gloss_id,
            gloss_idgloss,
            created_at,
            gloss_public.lower() == "t",
            video_public.lower() == "t",
            video_id,
        ]

    print(
        f"{len(this_nzsl_raw_keys_dict)} rows retrieved",
        file=sys.stderr,
    )

    return this_nzsl_raw_keys_dict


# Get the keys present and absent across NZSL Signbank and S3, to dictionary
# See DICTIONARY and CACHE FILE format
def create_all_keys_dict(this_s3_bucket_raw_keys_list, this_nzsl_raw_keys_dict):
    print(
        "Getting keys present and absent across NZSL Signbank and S3 ...",
        file=sys.stderr,
    )
    this_all_keys_dict = {}
    with open(ALL_KEYS_CACHE_FILE, "w") as cache_file:

        # Find S3 keys that are present in NZSL, or absent
        for video_key in this_s3_bucket_raw_keys_list:
            if video_key in this_nzsl_raw_keys_dict:
                # NZSL PRESENT, S3 PRESENT
                this_all_keys_dict[video_key] = [True, True] + this_nzsl_raw_keys_dict[
                    video_key
                ]
            else:
                # NZSL Absent, S3 PRESENT
                this_all_keys_dict[video_key] = [False, True, "", "", "", "", "", ""]

        # Find NZSL keys that are absent from S3 (present handled already above)
        for video_key, item_list in this_nzsl_raw_keys_dict.items():
            if video_key not in this_s3_bucket_raw_keys_list:
                # NZSL PRESENT, S3 Absent
                this_all_keys_dict[video_key] = [True, False] + item_list

        # Write all keys back to a cache file
        for video_key, item_list in this_all_keys_dict.items():
            cache_file.write(
                f"{video_key}{CSV_DELIMITER}{CSV_DELIMITER.join(map(str, item_list))}\n"
            )

    return this_all_keys_dict


def build_csv_header():
    return CSV_DELIMITER.join(
        [
            "Gloss ID",
            "Gloss",
            "Created at",
            "Gloss public",
            "Video public",
            "Video ID",
            "Video key",
            "Expected Canned ACL",
            "Actual Canned ACL",
        ]
    )


def build_csv_row(
    key_in_nzsl=False,
    key_in_s3=False,
    gloss_id=None,
    gloss_idgloss=None,
    created_at=None,
    gloss_public=False,
    video_public=False,
    video_id=None,
    video_key=None,
):

    # See signbank/video/models.py, line 59, in function set_public_acl()
    if key_in_nzsl:
        canned_acl_expected = "public-read" if video_public else "private"
    else:
        canned_acl_expected = ""

    # If key not in S3, just return its NZSL info
    if not key_in_s3:
        return CSV_DELIMITER.join(
            [
                f"{gloss_id}",
                f"{gloss_idgloss}",
                f"{created_at}",
                f"{gloss_public}",
                f"{video_public}",
                f"{video_id}",
                f"{video_key}",
                f"{canned_acl_expected}",
                "",
            ]
        )

    # Get S3 object's ACL
    result = subprocess.run(
        [
            AWSCLI,
            "s3api",
            "get-object-acl",
            "--output",
            "json",
            "--bucket",
            AWS_S3_BUCKET,
            "--key",
            video_key,
        ],
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
    elif acls_grants_json[0]["Permission"] == "FULL_CONTROL":
        canned_acl = "private"

    return CSV_DELIMITER.join(
        [
            f"{gloss_id}",
            f"{gloss_idgloss}",
            f"{created_at}",
            f"{gloss_public}",
            f"{video_public}",
            f"{video_id}",
            f"{video_key}",
            f"{canned_acl_expected}",
            f"{canned_acl}",
        ]
    )


# From the keys present in NZSL, get all their ACL information
def output_csv(this_all_keys_dict):
    print(f"Getting ACLs for keys from S3 ({AWS_S3_BUCKET}) ...", file=sys.stderr)

    print(build_csv_header())
    for video_key, [
        key_in_nzsl,
        key_in_s3,
        gloss_id,
        gloss_idgloss,
        created_at,
        gloss_public,
        video_public,
        video_id,
    ] in this_all_keys_dict.items():

        print(
            build_csv_row(
                key_in_nzsl,
                key_in_s3,
                gloss_id,
                gloss_idgloss,
                created_at,
                gloss_public,
                video_public,
                video_id,
                video_key,
            )
        )


print(f"Mode:      {args.env}", file=sys.stderr)
print(f"S3 bucket: {AWS_S3_BUCKET}", file=sys.stderr)
print(f"AWSCLI:    {AWSCLI}", file=sys.stderr)
print(f"PGCLI:     {PGCLI}", file=sys.stderr)
print(f"TMPDIR:    {TMPDIR}", file=sys.stderr)
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
