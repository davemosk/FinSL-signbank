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
import re
from time import sleep
from uuid import uuid4
from pprint import pprint


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
    "--awscli",
    default="/usr/local/bin/aws",
    required=False,
    help=f"AWS client path (default: %(default)s)",
)
parser.add_argument(
    "--pyenv",
    default=False,
    required=False,
    action="store_true",
    help=f"Yes, we are running in a pyenv virtualenv that has all the right site-packages installed",
)
args = parser.parse_args()

if args.pyenv:
    # Magic required to allow this script to use Signbank Django classes
    # This goes away if this script becomes a Django Management Command
    print("Importing site-packages environment", file=sys.stderr)
    print(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), file=sys.stderr)
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "signbank.settings.development")
    from django.core.wsgi import get_wsgi_application

    get_wsgi_application()

    from django.contrib.auth.models import Permission
    from django.contrib.auth import get_user_model

    User = get_user_model()

    from django.test import Client
    from django.core.files.uploadedfile import SimpleUploadedFile
    from django.urls import reverse
    from django.db.utils import IntegrityError
    from signbank.dictionary.models import (
        Dataset,
        FieldChoice,
        Gloss,
        GlossTranslations,
        Language,
        ManualValidationAggregation,
        ShareValidationAggregation,
        ValidationRecord,
    )
    from signbank.video.models import GlossVideo

# Globals
CSV_DELIMITER = ","
FAKEKEY_PREFIX = "this_is_not_a_key_"
DATABASE_URL = os.getenv("DATABASE_URL", "")
AWSCLI = args.awscli
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


def aws_cli(args_list):
    # Try indefinitely
    output = None
    while not output:
        try:
            output = subprocess.run(
                [AWSCLI] + args_list,
                env=os.environ,
                capture_output=True,
                check=True,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(
                f"Error: subprocess.run returned code {e.returncode}", file=sys.stderr
            )
            print(e.cmd, file=sys.stderr)
            print(e.stdout, file=sys.stderr)
            print(e.stderr, file=sys.stderr)
            sleep(1)
    return output


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
    result = aws_cli(
        [
            "s3",
            "ls",
            f"s3://{s3_bucket}",
            "--recursive",
        ],
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


def find_orphans():
    all_keys_dict = create_all_keys_dict(
        get_nzsl_raw_keys_dict(), get_s3_bucket_raw_keys_list()
    )

    print("Gloss ID,Gloss,Suggested Video key")

    # Traverse all the NZSL Signbank glosses that are missing S3 objects
    for video_key, [
        key_in_nzsl,
        key_in_s3,
        gloss_idgloss,
        gloss_created_at,
        gloss_id,
        video_id,
        gloss_public,
        video_public,
    ] in all_keys_dict.items():

        if not key_in_nzsl:
            # This is an S3 object, not a Signbank record
            continue

        if key_in_s3:
            # This Signbank record already has an S3 object, all is well
            continue

        # Business rule
        if int(gloss_id) < 8000:
            continue

        # The gloss_id is the only reliable retrieval key at the Signbank end
        gloss = Gloss.objects.get(id=gloss_id)
        video_path = gloss.get_video_path()

        # Skip any that already have a video path
        # If these had S3 video candidates they should not have made it this far
        # These will have to have their videos reinstated (separate operation)
        if len(video_path) > 0:
            continue

        gloss_name = gloss.idgloss.split(":")[0].strip()

        csv_rows = []

        # We try to find the orphaned S3 object, if it exists
        # TODO We could improve on brute-force by installing new libraries eg. rapidfuzz
        for test_key, [key_nzsl_yes, key_s3_yes, *_] in all_keys_dict.items():
            if gloss_name in test_key:
                if str(gloss_id) in test_key:
                    if key_nzsl_yes:
                        print(f"Anomaly (in NZSL): {gloss.idgloss}", file=sys.stderr)
                        continue
                    if not key_s3_yes:
                        print(f"Anomaly (not in S3): {gloss.idgloss}", file=sys.stderr)
                        continue
                    csv_rows.append([gloss_id, gloss.idgloss, test_key])
        if csv_rows:
            for c_row in csv_rows:
                print(CSV_DELIMITER.join(c_row))


print(f"Env:         {args.env}", file=sys.stderr)
print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
print(f"AWSCLI:      {AWSCLI}", file=sys.stderr)
print(f"PGCLI:       {PGCLI}", file=sys.stderr)
print(f"AWS profile: {os.environ.get('AWS_PROFILE', '')}", file=sys.stderr)

if args.pyenv:
    find_orphans()
else:
    print(
        "Error: You need to tell us you're in an environment with all needed site-packages. See --pyenv"
    )
