#!/usr/bin/env -S python3 -u
# You need to run this in a venv that has all the right Python site-packages.
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
import re
from time import sleep
from pprint import pprint
import boto3
import copy
import csv

# Magic required to allow this script to use Signbank Django classes
print(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
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

parser = argparse.ArgumentParser(
    description="You need to run this in a venv that has all the right Python site-packages. You must setup: An AWS auth means, eg. AWS_PROFILE env var. "
    "Postgres access details, eg. DATABASE_URL env var."
)
parser.add_argument(
    "--env",
    default="dev",
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


# Globals
CSV_DELIMITER = ","
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


# Run some tests against the remote endpoints
def do_tests():
    # Debugging safety
    if args.env != "dev":
        print("Error: tests must be in 'dev' environment")
        exit()
    if DATABASE_URL.find("@localhost") < 0:
        print("Error: database url must contain '@localhost'")
        exit()
    print(f"DATABASE_URL:{DATABASE_URL}")

    print("Running tests")
    # s3 = boto3.client("s3")
    # pprint(s3.list_objects(Bucket=AWS_S3_BUCKET))
    # get_nzsl_raw_keys_dict()
    # pprint(Gloss.objects.all())

    # This is a cut and paste of the mock tests, but we're doing it "live" on dev
    _csv_content = {
        "id": "111",
        "word": "Test",
        "maori": "maori, maori 2",
        "secondary": "test",
        "notes": "a note",
        "created_at": "2023-09-12 22:37:59 UTC",
        "contributor_email": "ops@ackama.com",
        "contributor_username": "Ackama Ops",
        "agrees": "0",
        "disagrees": "1",
        "topic_names": "Test Topic|Test",
        "videos": "/rails/active_storage/blobs/redirect/eyJfcmFpbHMiOnsibWVzc2FnZSI6IkJBaHBBc2pFIiwiZXhwIjoiMjAyNC0xMS0wM1QyMzoyNzo1Ni4yNDNaIiwicHVyIjoiYmxvYl9pZCJ9fQ==--53448dc4efcf056e7ba7fe6b711d6b1ae551d171/Zimbabwe.mp4",
        "illustrations": "/kiwifruit-2-6422.png",
        "usage_examples": "/fire.1923.finalexample1.mb.r480x360.mp4",
        "sign_comments": ("contribution_limit_test_1: Comment 0|Comment 33"),
    }
    file_name = "test.csv"
    csv_content = [copy.deepcopy(_csv_content)]
    csv_content[0]["id"] = "12345"
    with open(file_name, "w") as file:
        writer = csv.writer(file)
        writer.writerow(csv_content[0].keys())
        for row in csv_content:
            writer.writerow(row.values())
    data = open(file_name, "rb")
    file = SimpleUploadedFile(
        content=data.read(), name=data.name, content_type="content/multipart"
    )
    dataset = Dataset.objects.get(name="NZSL")

    try:
        Gloss.objects.get(idgloss="Share:11").delete()
    except ValueError:
        pass
    Gloss.objects.create(
        dataset=dataset,
        idgloss="Share:11",
        nzsl_share_id="12345",
    )

    # Create user and add permissions
    try:
        user = User.objects.create_user(username="test", email=None, password="test")
        csv_permission = Permission.objects.get(codename="import_csv")
        user.user_permissions.add(csv_permission)
    except IntegrityError:
        user = User.objects.get(username="test")

    # Create client with change_gloss permission.
    client = Client()
    client.force_login(user)
    s = client.session
    s.update({"dataset_id": dataset.pk, "glosses_new": csv_content})
    s.save()
    response = client.post(
        reverse("dictionary:confirm_import_nzsl_share_gloss_csv"), {"confirm": True}
    )

    # test to see if we have to wait for thread
    X_SECONDS = 20
    print(f"Sleeping {X_SECONDS} seconds to allow threads to complete ...")
    sleep(X_SECONDS)


print(f"Env:         {args.env}", file=sys.stderr)
print(f"S3 bucket:   {AWS_S3_BUCKET}", file=sys.stderr)
print(f"AWSCLI:      {AWSCLI}", file=sys.stderr)
print(f"PGCLI:       {PGCLI}", file=sys.stderr)
print(f"AWS profile: {os.environ.get('AWS_PROFILE', '')}", file=sys.stderr)

do_tests()
