#!/usr/bin/env -S python3 -u
# Bang line above passes '-u' to python, for unbuffered output
# Permissions required:
#  psql - access to heroku app's postgres
#  aws s3 - NZSL IAM access
#  s3:GetObjectAcl permissions or READ_ACP access to the object
#  https://docs.aws.amazon.com/cli/latest/reference/s3api/get-object-acl.html
# For some commands you need to run this in a venv that has all the right Python site-packages.
# TODO Convert this script to a Django Management Command

import os
import sys
import subprocess
import argparse
from uuid import uuid4
from pprint import pprint
import boto3

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
    "--pgcli",
    default="/usr/bin/psql",
    required=False,
    help=f"Postgres client path (default: %(default)s)",
)
parser.add_argument(
    "--dumpnzsl",
    default=False,
    required=False,
    action="store_true",
    help=f"Dump raw NZSL database output",
)
parser.add_argument(
    "--dumps3",
    default=False,
    required=False,
    action="store_true",
    help=f"Dump raw S3 keys output",
)
args = parser.parse_args()

# Globals
CSV_DELIMITER = ","
FAKEKEY_PREFIX = "this_is_not_a_key_"
DATABASE_URL = os.getenv("DATABASE_URL", "")
PGCLI = args.pgcli
AWS_S3_BUCKET = f"nzsl-signbank-media-{args.env}"


def pg_cli(args_list):
    try:
        return subprocess.run(
            [PGCLI, "-c"] + args_list + [f"{DATABASE_URL}"],
            env=os.environ,
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error: subprocess.run returned code {e.returncode}", file=sys.stderr)
        print(e.cmd, file=sys.stderr)
        print(e.stdout, file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        exit()


# Fake key is a hack to handle FULL JOIN
def maybe_fakekey(instring):
    return instring if instring else FAKEKEY_PREFIX + str(uuid4())


def filter_fakekey(instring):
    return "" if instring.startswith(FAKEKEY_PREFIX) else instring


# Get the video files info from NZSL Signbank
def get_nzsl_raw_keys_dict():
    print(
        f"Getting raw list of video file info from NZSL Signbank ...",
        file=sys.stderr,
    )
    this_nzsl_raw_keys_dict = {}
    # Column renaming is for readability
    # Special delimiter because columns might contain commas
    result = pg_cli(
        [
            "COPY ("
            "SELECT "
            "dg.id AS gloss_id, "
            "dg.idgloss AS gloss_idgloss, "
            "dg.created_at AS gloss_created_at, "
            "dg.published AS gloss_public, "
            "vg.is_public AS video_public, "
            "vg.id AS video_id, "
            "vg.videofile AS video_key "
            "FROM dictionary_gloss AS dg "
            "FULL JOIN video_glossvideo AS vg ON vg.gloss_id = dg.id"
            ") TO STDOUT WITH (FORMAT CSV, DELIMITER '|')",
        ]
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
            gloss_created_at,
            gloss_public,
            video_public,
            video_id,
            video_key,
        ] = rawl.split("|")

        # Hack to handle FULL JOIN
        video_key = maybe_fakekey(video_key.strip())

        # This sets the initial field ordering in the all_keys dictionary row
        this_nzsl_raw_keys_dict[video_key] = [
            gloss_idgloss.replace(CSV_DELIMITER, ""),
            gloss_created_at,
            gloss_id,
            video_id,
            gloss_public.lower() == "t",
            video_public.lower() == "t",
        ]

    print(
        f"{len(this_nzsl_raw_keys_dict)} rows retrieved",
        file=sys.stderr,
    )

    return this_nzsl_raw_keys_dict


# Get all keys from AWS S3
def get_s3_bucket_raw_keys_list(s3_bucket=AWS_S3_BUCKET):
    print(f"Getting raw AWS S3 keys recursively ({s3_bucket}) ...", file=sys.stderr)

    s3_resource = boto3.resource("s3")
    s3_resource_bucket = s3_resource.Bucket(s3_bucket)
    this_s3_bucket_raw_keys_list = [
        s3_object.key for s3_object in s3_resource_bucket.objects.all()
    ]

    print(
        f"{len(this_s3_bucket_raw_keys_list)} rows retrieved",
        file=sys.stderr,
    )

    return this_s3_bucket_raw_keys_list


# Get the keys present and absent across NZSL Signbank and S3, to dictionary
def create_all_keys_dict(this_nzsl_raw_keys_dict, this_s3_bucket_raw_keys_list):
    print(
        "Getting keys present and absent across NZSL Signbank and S3 ...",
        file=sys.stderr,
    )
    this_all_keys_dict = {}

    # Find S3 keys that are present in NZSL, or absent
    for video_key in this_s3_bucket_raw_keys_list:
        dict_row = this_nzsl_raw_keys_dict.get(video_key, None)
        if dict_row:
            this_all_keys_dict[video_key] = [
                True,  # NZSL PRESENT
                True,  # S3 PRESENT
            ] + dict_row
        else:
            this_all_keys_dict[video_key] = [
                False,  # NZSL Absent
                True,  # S3 PRESENT
            ] + [""] * 6

    # Find NZSL keys that are absent from S3 (present handled above)
    for video_key, dict_row in this_nzsl_raw_keys_dict.items():
        if video_key not in this_s3_bucket_raw_keys_list:
            this_all_keys_dict[video_key] = [
                True,  # NZSL PRESENT
                False,  # S3 Absent
            ] + dict_row

    return this_all_keys_dict


# Cases
# In S3     In NZSL     Action
#   Is        Not         Delete S3 Object
#   Is        Is          Update ACL
#   Not       Is          Review
#      Other              Review
def get_recommended_action(key_in_nzsl, key_in_s3):
    if key_in_s3:
        if key_in_nzsl:
            return "Update ACL"
        else:
            return "Delete S3 Object"
    return "Review"


# Get S3 object's ACL
def get_s3_canned_acl(video_key):
    # TODO pass in a boto client instead of recreating one each time
    s3_client = boto3.client("s3")
    acls_grants = s3_client.get_object_acl(Bucket=AWS_S3_BUCKET, Key=video_key)[
        "Grants"
    ]
    if len(acls_grants) > 1:
        if (
            acls_grants[0]["Permission"] == "FULL_CONTROL"
            and acls_grants[1]["Permission"] == "READ"
        ):
            return "public-read"
    elif acls_grants[0]["Permission"] == "FULL_CONTROL":
        return "private"

    return "unknown"


# Get S3 object's LastModified date/time
def get_s3_lastmodified(video_key):
    # TODO pass in a boto client instead of recreating one each time
    return boto3.client("s3").head_object(Bucket=AWS_S3_BUCKET, Key=video_key)[
        "LastModified"
    ]


def build_csv_header():
    return CSV_DELIMITER.join(
        [
            "Action",
            "S3 Video key",
            "S3 LastModified",
            "S3 Expected Canned ACL",
            "S3 Actual Canned ACL",
            "Sbank Gloss ID",
            "Sbank Video ID",
            "Sbank Gloss public",
            "Sbank Video public",
            "Sbank Gloss",
            "Sbank Gloss created at",
        ]
    )


def build_csv_row(
    video_key,
    key_in_nzsl=False,
    key_in_s3=False,
    gloss_idgloss=None,
    gloss_created_at=None,
    gloss_id=None,
    video_id=None,
    gloss_public=False,
    video_public=False,
):
    # See signbank/video/models.py, line 59, function set_public_acl()
    canned_acl_expected = ""
    if key_in_nzsl:
        canned_acl_expected = "public-read" if video_public else "private"

    lastmodified = ""
    canned_acl = ""
    if key_in_s3:
        lastmodified = get_s3_lastmodified(video_key)
        canned_acl = get_s3_canned_acl(video_key)

    action = get_recommended_action(key_in_nzsl, key_in_s3)

    return CSV_DELIMITER.join(
        [
            action,
            f"{filter_fakekey(video_key)}",
            f"{lastmodified}",
            f"{canned_acl_expected}",
            f"{canned_acl}",
            f"{gloss_id}",
            f"{video_id}",
            f"{gloss_public}",
            f"{video_public}",
            f"{gloss_idgloss}",
            f"{gloss_created_at}",
        ]
    )


# From the keys present in NZSL, get all their S3 information
def process_keys(this_all_keys_dict):
    print(f"Getting detailed S3 data for keys ({AWS_S3_BUCKET}) ...", file=sys.stderr)

    print(build_csv_header())

    for video_key, dict_row in this_all_keys_dict.items():
        print(build_csv_row(video_key, *dict_row))


print(f"Env:         {args.env}", file=sys.stderr)
print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
print(f"PGCLI:       {PGCLI}", file=sys.stderr)
print(f"AWS profile: {os.environ.get('AWS_PROFILE', '')}", file=sys.stderr)

if args.dumpnzsl:
    pprint(get_nzsl_raw_keys_dict())
    exit()

if args.dumps3:
    pprint(get_s3_bucket_raw_keys_list())
    exit()

process_keys(
    create_all_keys_dict(get_nzsl_raw_keys_dict(), get_s3_bucket_raw_keys_list())
)
