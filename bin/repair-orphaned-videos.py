#!/usr/bin/env -S python3 -u
#
# This script needs to be run in a pyenv virtualenv with the Django project installed.
#
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
import csv
import subprocess
import argparse
from time import sleep
from pprint import pprint

# Magic required to allow this script to use Signbank Django classes
# This goes away if this script becomes a Django Management Command
print("Importing site-packages environment", file=sys.stderr)
print(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), file=sys.stderr)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "signbank.settings.development")
from django.core.wsgi import get_wsgi_application

get_wsgi_application()

from django.contrib.auth import get_user_model

User = get_user_model()

from signbank.dictionary.models import (
    FieldChoice,
    Gloss,
)
from signbank.video.models import GlossVideo

from django.core.exceptions import ObjectDoesNotExist


parser = argparse.ArgumentParser(
    description="You must setup: An AWS auth means, eg. AWS_PROFILE env var. "
    "Postgres access details, eg. DATABASE_URL env var."
)

# Positional arguments
parser.add_argument("csv_filename", help="Name of CSV file, or '-' for STDIN")

# Optional arguments
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
args = parser.parse_args()

# Keep synced with other scripts
GLOSS_ID_COLUMN = "Gloss ID"
GLOSS_COLUMN = "Gloss"
GLOSS_VIDEO_COLUMN = "Suggested Video key"
GLOBAL_COLUMN_HEADINGS = [GLOSS_ID_COLUMN, GLOSS_COLUMN, GLOSS_VIDEO_COLUMN]

# Other globals
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


# Returns a list of dictionaries, one for each CSV row
def read_csv(csv_filename):
    if csv_filename == "-":
        f = sys.stdin.read().splitlines()
    else:
        f = open(csv_filename, "r")
    return csv.DictReader(f)


def process_csv():
    main_video_type = FieldChoice.objects.filter(
        field="video_type", english_name="main"
    ).first()

    csv_rows = read_csv(args.csv_filename)
    for csv_row in csv_rows:
        gloss_id = csv_row[GLOSS_ID_COLUMN]
        gloss_idgloss = csv_row[GLOSS_COLUMN]
        video_key = csv_row[GLOSS_VIDEO_COLUMN]
        print(CSV_DELIMITER.join([gloss_id, gloss_idgloss, video_key]))
        gloss_id = int(gloss_id)

        try:
            gloss = Gloss.objects.get(id=gloss_id)
            print(gloss)
        except ObjectDoesNotExist as e:
            print(e)
            continue

        try:
            GlossVideo.objects.get(videofile=video_key)
            print(f"Error: GlossVideo already exists: {video_key}")
            continue
        except ObjectDoesNotExist:
            pass

        gloss_video = GlossVideo(
            gloss=gloss,
            dataset=gloss.dataset,
            videofile=video_key,
            title=video_key,
            version=0,
            is_public=False,
            video_type=main_video_type,
        )
        print(gloss_video)

        # At this point we complete the repair
        # WARNING, it tries to save to the current storage medium, so this needs sorting out!
        # We absolutely DO NOT want it to try and save!
        # save() is overridden in the GlossVideo model
        # Hm, maybe we SHOULD just write to the database after all, and hope Django copes?
        # Yeah, starting to think that's the way to go, IF postgres will allow us to do so (constraints)
        #gloss_video.save()


print(f"Env:         {args.env}", file=sys.stderr)
print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
print(f"AWSCLI:      {AWSCLI}", file=sys.stderr)
print(f"PGCLI:       {PGCLI}", file=sys.stderr)
print(f"AWS profile: {os.environ.get('AWS_PROFILE', '')}", file=sys.stderr)


process_csv()
