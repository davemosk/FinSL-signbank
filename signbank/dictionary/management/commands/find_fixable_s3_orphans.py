#!/usr/bin/env -S python3 -u
#
# Finds orphaned S3 objects that can be matched back to NZSL entries that are missing S3 objects.
# Essentially finds one form of import error.
#
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
import boto3
import csv
from signbank.dictionary.models import Gloss


# Keep synced with other scripts
GLOSS_ID_COLUMN = "Gloss ID"
GLOSS_COLUMN = "Gloss"
GLOSS_PUBLIC_COLUMN = "Gloss public"
GLOSS_VIDEO_COLUMN = "Suggested Video key"
GLOBAL_COLUMN_HEADINGS = [
    GLOSS_ID_COLUMN,
    GLOSS_COLUMN,
    GLOSS_PUBLIC_COLUMN,
    GLOSS_VIDEO_COLUMN,
]

# Other globals
CSV_DELIMITER = ","
FAKEKEY_PREFIX = "this_is_not_a_key_"
DATABASE_URL = os.getenv("DATABASE_URL", "")
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

    s3_resource = boto3.resource("s3")
    s3_resource_bucket = s3_resource.Bucket(AWS_S3_BUCKET)
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
            # NZSL glossvideo record for this S3 key
            this_all_keys_dict[video_key] = [
                True,  # NZSL PRESENT
                True,  # S3 PRESENT
            ] + dict_row
        else:
            # S3 key with no corresponding NZSL glossvideo record
            this_all_keys_dict[video_key] = [
                False,  # NZSL Absent
                True,  # S3 PRESENT
            ] + [""] * 6

    # Find NZSL keys that are absent from S3 (present in both handled above)
    for video_key, dict_row in this_nzsl_raw_keys_dict.items():
        if video_key not in this_s3_bucket_raw_keys_list:
            # gloss/glossvideo record with no corresponding S3 key
            # Either:
            # video_key is real, but the S3 object is missing
            # video_key is fake (to handle the FULL JOIN) and this gloss/glossvideo never had an S3 object
            this_all_keys_dict[video_key] = [
                True,  # NZSL PRESENT
                False,  # S3 Absent
            ] + dict_row

    return this_all_keys_dict


def find_orphans():
    all_keys_dict = create_all_keys_dict(
        get_nzsl_raw_keys_dict(), get_s3_bucket_raw_keys_list()
    )
    print("Finding fixable orphans", file=sys.stderr)

    out = csv.writer(sys.stdout, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_NONE)
    out.writerow(GLOBAL_COLUMN_HEADINGS)

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

        # The gloss_id is the only reliable retrieval key at the Signbank end
        gloss = Gloss.objects.get(id=gloss_id)
        gloss_name = gloss.idgloss.split(":")[0].strip()

        # Skip any that already have a video path
        # These should have an S3 object but don't: For some reason the video never made it to S3
        # These will have to have their videos reinstated (separate operation)
        if gloss.glossvideo_set.exists():
            continue

        # We try to find the orphaned S3 object, if it exists
        # TODO We could improve on brute-force by installing new libraries eg. rapidfuzz
        for test_key, [key_nzsl_yes, key_s3_yes, *_] in all_keys_dict.items():
            if test_key.startswith(FAKEKEY_PREFIX):
                continue
            if gloss_name in test_key:
                if str(gloss_id) in test_key:
                    if key_nzsl_yes:
                        print(f"Anomaly (in NZSL): {gloss.idgloss}", file=sys.stderr)
                        continue
                    if not key_s3_yes:
                        print(f"Anomaly (not in S3): {gloss.idgloss}", file=sys.stderr)
                        continue
                    out.writerow([gloss_id, gloss.idgloss, str(gloss_public), test_key])


class Command(BaseCommand):
    help = (
        "Find orphaned S3 objects that can be matched back to NZSL entries that are missing S3 objects. "
        "You must setup: An AWS auth means, eg. AWS_PROFILE env var. "
        "Postgres access details, eg. DATABASE_URL env var."
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

    def handle(self, *args, **options):
        global PGCLI, AWS_S3_BUCKET
        PGCLI = options["pgcli"]
        AWS_S3_BUCKET = f"nzsl-signbank-media-{options['env']}"

        print(f"Env:         {options['env']}", file=sys.stderr)
        print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
        print(f"PGCLI:       {PGCLI}", file=sys.stderr)
        print(f"AWS profile: {os.environ.get('AWS_PROFILE', '')}", file=sys.stderr)

        find_orphans()
