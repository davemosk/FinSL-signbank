## Diagnosing and manipulating S3/Postgres video relatioinships

There are 3 scripts to help with this.

These are used to report on relationships between Signbank's Postgres database and Amazon's S3 file storage, and then
_assist_ with effecting some types of repair where discrepancies exist.

Only some actions are performed by these scripts, other operations have to be manually scripted using the AWS `cli` or
other means, using the output from these scripts as data.

The scripts use the `Boto3` python library to talk to AWS S3.
They use an external client to talk to Postgres.

They output diagnostic and progress information on STDERR.
All data output is on STDOUT and may be safely redirected.

The scripts require, usually in the environment:

- An AWS profile - eg. `AWS_PROFILE` environment variable set to a pre-configured profile.
- A Postgres context - eg. `DATABASE_URL` environment variable with target and credentials.

The scripts have common arguments:

- `--help` or `-h` - emit a Help message showing the available arguments.
- `--env` - specifies the target environment, eg. `dev`, `uat`, `production`. This is used to contruct the name of the
  AWS S3 bucket name, eg. `nzsl-signbank-media-uat`. The default is `uat`.
- `--pgcli` - allows the user to specify a different path for the Postgres command-line client. The default is
  `/usr/bin/psql`.

<br />

### get-video-s3-acls.py

This script has extra arguments:

- `--dumpnzsl` Just get the NZSL Signbank database contents, output it, then exit. Mainly for debugging.
- `--dumps3` Just get the AWS S3 contents, output it, then exit. Mainly for debugging.

This script produces a full report on Postgres vs S3.
It outputs as CSV, with headers.
The columns are as follows:

```
Action
S3 Video key
S3 LastModified
S3 Expected Canned ACL
S3 Actual Canned ACL
Sbank Gloss ID
Sbank Video ID
Sbank Gloss public
Sbank Video public
Sbank Gloss
Sbank Gloss created at
```

`Action` is a fix suggested by the script.

`Action` is one of:

- `Delete S3 Object`

The S3 object is "orphaned", that is, it has no corresponding NZSL Signbank postgres database record. Some of these are
fixable, see the `find-fixable-s3-orphans.py` script. But any that are not should be deleted as they are taking up space
without being visible to the NZSL Signbank application.

- `Update ACL`

Make sure that the S3 object's ACL matches the expected value in the column next to it, and fix it if not.
This uses AWS *Canned ACLs*, which in our case means the two values `private` and `public-read`.

- `Review`

Usually means there is a Signbank NZSL database entry with no corresponding S3 object. These are out of scope for these
scripts, and are expected to be fixed by other means (eg. functionality within the NZSL Signbank app).

Example usage:

```
This example will access a local postgres port, an AWS account specified by the AWS profile 'nzsl',   
and an AWS S3 bucket called 'nzsl-signbank-media-dev' and output the resulting CSV to a text file 'dev.csv'.

export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres"
export AWS_PROFILE=nzsl

get-video-s3-acls.py --env dev > dev.csv
```

<br />

### find-fixable-s3-orphans.py

This script accesses the database and S3 in a similar way to `get-video-s3-acls.py`.
(Dev note: It contains a lot of duplicated code with that script, which should be libratised at some point.)

It finds S3 objects that have no corresponding NZSL Signbank database record. These are 'orphaned' S3 objects.
It then parses the name string of the object and attempts to find an NZSL Signbank record that matches it. This is not
guaranteed to be correct, so the output needs human review.
It outputs what it finds as CSV with header, in a format that can be digested by the 3rd script
`repair-fixable-s3-orphans.py`.



<br />

### repair-fixable-s3-orphans.py

This attempts to unify NZSL Signbank records with S3 orphans, by digesting a CSV input of the same format as output by
`find-fixable-orphans.py`. It does this by generating `GlossVideo` Django objects where necessary, and associating them
with the correct `Gloss` Django objects. This operation _changes_ the database contents and so must be used with
caution.



