#!/usr/bin/env -S python3 -u
# Bang line above passes '-u' to python, for unbuffered output
# Permissions required:
#  psql - access to heroku app's postgres
#  aws s3 - NZSL IAM access
#  s3:GetObjectAcl permissions or READ_ACP access to the object
#  https://docs.aws.amazon.com/cli/latest/reference/s3api/get-object-acl.html

from django.core.management.base import BaseCommand
import os
import sys
import subprocess
from uuid import uuid4
from pprint import pprint
import boto3
import csv


# Globals
CSV_DELIMITER = ","
FAKEKEY_PREFIX = "this_is_not_a_key_"
DATABASE_URL = os.getenv("DATABASE_URL", "")
S3_CLIENT = boto3.client("s3")
S3_RESOURCE = boto3.resource("s3")
PGCLI = "/usr/bin/psql"
AWS_S3_BUCKET = ""


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

        """
        Hack to handle FULL JOIN.
        We are storing data rows in a dictionary, indexed by video_key.
        Because we are doing a FULL JOIN on the NZSL Signbank database,
        we also get rows where there are gloss entries that do not have
        a corresponding video_glossvideo.
        (These are erroneous and one of the reasons this script exists,
        to find them.)
        Consequently there is no video_key, and we cannot use it to index
        the data row.
        Instead, we create a fake video_key that is unique and, theoretically,
        impossible for anything else to try and use. It also has a 'safe',
        easily filtered prefix, which means later code can easily tell
        a fake key from a real key.
        Always having a key, in this way, means that code, eg. loops,
        that depends on there being a dictionary key axis will not break.
        """
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
def get_s3_bucket_raw_keys_list():
    print(f"Getting raw AWS S3 keys recursively ({AWS_S3_BUCKET}) ...", file=sys.stderr)

    s3_resource_bucket = S3_RESOURCE.Bucket(AWS_S3_BUCKET)
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
    # TODO This could be changed to use pop(), so that on each pass we are left
    # with a smaller subset of the rows, which we can search faster. If the
    # database becomes very large in future this could save a lot of processing.
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


def get_recommended_action(key_in_nzsl, key_in_s3):
    """
    Cases
    In S3     In NZSL     Action
      Is        Is          Update ACL
      Is        Not         Delete S3 Object
      Not       --          Review
    """
    if key_in_s3:
        if key_in_nzsl:
            return "Update ACL"
        else:
            return "Delete S3 Object"
    return "Review"


# Get S3 object's ACL
def get_s3_canned_acl(video_key):
    acls_grants = S3_CLIENT.get_object_acl(Bucket=AWS_S3_BUCKET, Key=video_key)[
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
    return S3_CLIENT.head_object(Bucket=AWS_S3_BUCKET, Key=video_key)["LastModified"]


def build_csv_header():
    return [
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

    return [
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


# From the keys present in NZSL, get all their S3 information
def process_keys(this_all_keys_dict):
    print(f"Getting detailed S3 data for keys ({AWS_S3_BUCKET}) ...", file=sys.stderr)

    out = csv.writer(sys.stdout, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_NONE)
    out.writerow(build_csv_header())

    for video_key, dict_row in this_all_keys_dict.items():
        out.writerow(build_csv_row(video_key, *dict_row))


class Command(BaseCommand):
    help = (
        "Get all S3 bucket video object and recommends actions for them. "
        "You must setup: (1) An AWS auth means, eg. AWS_PROFILE env var. "
        "(2) Postgres access details, eg. DATABASE_URL env var."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--env",
            default="uat",
            required=False,
            help="Environment to run against, eg 'production, 'uat', etc (default: '%(default)s')",
        )
        parser.add_argument(
            "--pgcli",
            default=PGCLI,
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

    def handle(self, *args, **options):
        global PGCLI, AWS_S3_BUCKET
        PGCLI = options["pgcli"]
        AWS_S3_BUCKET = f"nzsl-signbank-media-{options['env']}"

        print(f"Env:         {options['env']}", file=sys.stderr)
        print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
        print(f"PGCLI:       {PGCLI}", file=sys.stderr)
        print(f"AWS profile: {os.environ.get('AWS_PROFILE', '')}", file=sys.stderr)

        if options["dumpnzsl"]:
            pprint(get_nzsl_raw_keys_dict())
            exit()

        if options["dumps3"]:
            pprint(get_s3_bucket_raw_keys_list())
            exit()

        process_keys(
            create_all_keys_dict(
                get_nzsl_raw_keys_dict(), get_s3_bucket_raw_keys_list()
            )
        )
