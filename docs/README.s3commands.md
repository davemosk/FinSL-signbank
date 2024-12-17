# Diagnosing and manipulating S3/Postgres video relationships

<br />

There are 3 Management Commands to help with this.

These commands are used to report on relationships between Signbank's Postgres database and Amazon's S3 file storage,
and then
_assist_ with effecting some types of repair where discrepancies exist.

Only some actions are performed by these commands, other operations have to be manually commanded using the AWS `cli` or
other means, using the output from these commands as data.

The commands use the `Boto3` python library to talk to AWS S3.
They use an external client to talk to Postgres.

They output diagnostic and progress information on STDERR.
All data output is on STDOUT and may be safely redirected.

The commands require, usually in the environment:

- An AWS profile - eg. `AWS_PROFILE` environment variable set to a pre-configured profile.
- A Postgres context - eg. `DATABASE_URL` environment variable with target and credentials.

The commands have common arguments:

- `--help` or `-h` - emit a Help message showing the available arguments.
- `--env` - specifies the target environment, eg. `dev`, `uat`, `production`. This is used to contruct the name of the
  AWS S3 bucket name, eg. `nzsl-signbank-media-uat`. The default is `uat`.
- `--pgcli` - allows the user to specify a different path for the Postgres command-line client. The default is
  `/usr/bin/psql`.

<br />

### get_video_s3_acls

This command has extra arguments:

- `--dumpnzsl` Just get the NZSL Signbank database contents, output it, then exit. Mainly for debugging.
- `--dumps3` Just get the AWS S3 contents, output it, then exit. Mainly for debugging.

This command produces a full report on Postgres vs S3.
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

`Action` is a fix suggested by the command.

`Action` is one of:

- `Delete S3 Object`

The S3 object is "orphaned", that is, it has no corresponding NZSL Signbank postgres database record. Some of these are
fixable, see the `find-fixable-s3-orphans.py` command. But any that are not should be deleted as they are taking up
space
without being visible to the NZSL Signbank application.

- `Update ACL`

Make sure that the S3 object's ACL matches the expected value in the column next to it, and fix it if not.
This uses AWS *Canned ACLs*, which in our case means the two values `private` and `public-read`.

- `Review`

Usually means there is a Signbank NZSL database entry with no corresponding S3 object. These are out of scope for these
commands, and are expected to be fixed by other means (eg. functionality within the NZSL Signbank app).

<br />

Example usage:

```
This example will access a local postgres port, an AWS account specified by the AWS profile 'nzsl',   
and an AWS S3 bucket called 'nzsl-signbank-media-dev' and output the resulting CSV to a text file 'dev.csv'.

export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres"
export AWS_PROFILE=nzsl

bin/develop.py get-video-s3-acls --env dev > dev.csv
```

<br />

Example output STDERR:

(Note that `DATABASE_URL` is never output by these commands, for security reasons)

```
Env:         dev
S3 bucket:   nzsl-signbank-media-dev
PGCLI:       /usr/bin/psql
AWS profile: nzsl
Getting raw list of video file info from NZSL Signbank ...
17470 rows retrieved
Getting raw AWS S3 keys recursively (nzsl-signbank-media-dev) ...
17442 rows retrieved
Getting keys present and absent across NZSL Signbank and S3 ...
Getting detailed S3 data for keys (nzsl-signbank-media-dev) ...
```

<br />

Example output STDOUT (i.e. CSV):

```
tail -f dev.csv
Action,S3 Video key,S3 LastModified,S3 Expected Canned ACL,S3 Actual Canned ACL,Sbank Gloss ID,Sbank Video ID,Sbank Gloss public,Sbank Video public,Sbank Gloss,Sbank Gloss created at
Update ACL,8271-Coronavirus (2 signs).8271_video.mov,2024-11-11 03:52:32+00:00,private,private,8271,19804,False,False,Coronavirus (2 signs):8271,2024-03-25 20:45:34.050097+00
Update ACL,8272-Coronavirus.8272_video.mov,2024-11-11 03:52:32+00:00,private,private,8272,19658,False,False,Coronavirus:8272,2024-03-25 20:45:34.050294+00
Update ACL,8273-organic.8273_usageexample_1.mp4,2024-11-11 03:52:32+00:00,private,private,8273,19890,False,False,organic:8273,2024-03-25 20:45:34.050454+00
Update ACL,8273-organic.8273_usageexample_2.mp4,2024-11-11 03:52:32+00:00,private,private,8273,19891,False,False,organic:8273,2024-03-25 20:45:34.050454+00
Update ACL,8273-organic.8273_video.mp4,2024-11-11 03:52:33+00:00,private,private,8273,19892,False,False,organic:8273,2024-03-25 20:45:34.050454+00
...
```

<br />

### find_fixable_s3_orphans

This command accesses the database and S3 in a similar way to `get_video_s3_acls`

(Dev note: It contains a lot of duplicated code with that command, which should be libratised at some point.)

It finds S3 objects that have no corresponding NZSL Signbank database record. These are 'orphaned' S3 objects.

It then parses the name string of the object and attempts to find an NZSL Signbank record that matches it.

This is not guaranteed to be correct, so the output needs human review.

It outputs what it finds as CSV with header, in a format that can be digested by the 3rd command
`repair_fixable_s3_orphans`.

The output columns are as follows:

`Gloss ID`
The command `repair_fixable_s3_orphans` uses this to find the Gloss and connect it to GlossVideo records

`Gloss`
This is required by `repair_fixable_s3_orphans` but is mainly for human readability

`Gloss public`
This is mainly for debugging, and is ignored by `repair_fixable_s3_orphans`

`Suggested Video key`
This is the closest matching video key the command found for the `Gloss`


<br />

Example usage:

```
bin/develop.py find_fixable_s3_orphans --env dev > orphans.csv
```

<br />

Example output STDERR:

```
Env:         dev
S3 bucket:   nzsl-signbank-media-dev
PGCLI:       /usr/bin/psql
AWS profile: nzsl
Getting raw list of video file info from NZSL Signbank ...
17470 rows retrieved
Getting raw AWS S3 keys recursively (nzsl-signbank-media-dev) ...
20035 rows retrieved
Getting keys present and absent across NZSL Signbank and S3 ...
Finding fixable orphans
```

<br />

Example output STDOUT:

```
tail -f orphans.csv
Gloss ID,Gloss,Gloss public,Suggested Video key
8274,metaphor:8274,8274-Metaphor.8274_usageexample_1.mp4
8274,metaphor:8274,8274-Metaphor.8274_usageexample_2.mp4
8274,metaphor:8274,8274-Metaphor.8274_video.mp4
8319,ecosystem:8319,8319-ecosystem.8319_usageexample_1.mov
...
```

<br />

### repair_fixable_s3_orphans

This command attempts to unify NZSL Signbank records with S3 orphans, by digesting a CSV input of the same format as
output by
`find-fixable-orphans.py`. It does this by generating `GlossVideo` Django objects where necessary, and associating them
with the correct `Gloss` Django objects.

This operation _changes_ the database contents and so must be used with
caution.

The CSV file is supplied as a non-optional positional argument.

**Important**

**--commit**

*The default behaviour is dry-run, i.e. the command will make no database changes.*
*If you provide the optional argument*

`--commit`

*then the command will make database changes.*

<br />

The command always outputs its inputs first, to expose what it is trying to do.

If the command is able to successfully create the new `GlossVideo` object, it will output the details of the `Gloss` and
the new `GlossVideo`

If, however, the command cannot create a new `GlossVideo`, it will output the reason. Usually this is
`GlossVideo already exists`

<br />

Example usage:

```
bin/develop.py repair_fixable_s3_orphans --env dev orphans.csv
```

<br />

Example output STDERR:

```
Env:         dev
S3 bucket:   nzsl-signbank-media-dev
PGCLI:       /usr/bin/psql
AWS profile: nzsl
Input file:  orphans.csv
Mode:        Dry-run
```

<br />

Example output STDOUT:

```
8274,metaphor:8274,8274-Metaphor.8274_usageexample_1.mp4
Ignoring: GlossVideo already exists: 8274-Metaphor.8274_usageexample_1.mp4
8274,metaphor:8274,8274-Metaphor.8274_usageexample_2.mp4
Ignoring: GlossVideo already exists: 8274-Metaphor.8274_usageexample_2.mp4
8274,metaphor:8274,8274-Metaphor.8274_video.mp4
Ignoring: GlossVideo already exists: 8274-Metaphor.8274_video.mp4
8319,ecosystem:8319,8319-ecosystem.8319_usageexample_1.mov
Ignoring: GlossVideo already exists: 8319-ecosystem.8319_usageexample_1.mov
...
```

# Bash script recipes for making Postgres/S3 changes

*The following code snippets were used in practice, but are offered here as examples.*

*They are dangerous, and must be understood before use.*



## Marked for deletion: glossvideo/ with id's below 8000

The file `uat.csv` was generated by command `get_video_s3_acls`

```
grep "Delete S3 Object" uat.csv | grep 'glossvideo/' | cut -d',' -f2 | grep -P '[0-7][0-9]{3,3}'
```


### Actual deletion: of the above
```
grep "Delete S3 Object" uat.csv | grep 'glossvideo/' | cut -d',' -f2 | grep -P '[0-7][0-9]{3,3}' | while read key ; do echo "$key" ; aws s3api delete-object --bucket 'nzsl-signbank-media-uat' --key "$key"; done
```

### Actual deletion: any marked for deletion .png files with 'picture' in the name
```
grep "Delete S3 Object" uat.csv | cut -d',' -f2 | grep -P '[0-7][0-9]{3,3}' | grep -e "picture.*png$" | while read key ; do echo "$key" ; aws s3api delete-object --bucket 'nzsl-signbank-media-uat' --key "$key"; done
```

<br />

## De-orphaning

The file `orphans-uat.csv` was generated using command `find_fixable_s3_orphans`, then *human reviewed and edited*.

```
./repair-fixable-orphans.py --env uat orphans-uat.csv
```

<br />

## Pause - you must regenerate the csv before the next steps

This is because you have de-orphaned some of the S3 objects

Here in our examples this file is named `uat.csv`

<br />

## DESTRUCTIVE STEP: Deletion of final S3 orphans ('non-fixable' S3 orphans)

### Safety dry-run (just get-object-attributes)
```
grep "Delete S3 Object" uat.csv | cut -d',' -f2 | while read key ; do aws s3api get-object-attributes --object-attributes ObjectParts --bucket 'nzsl-signbank-media-uat' --key "$key"; done
```

### Actual deletion
```
grep "Delete S3 Object" uat.csv | cut -d',' -f2 | while read key ; do echo "$key" ; aws s3api delete-object --bucket 'nzsl-signbank-media-uat' --key "$key"; done
```

<br />

## Final step: Updating ACLs

The following script was created, and processes the output `uat.csv` from `get_video_s3_acls`

```
#!/bin/bash
# fix-acls.sh
# Fix ACLS using 'canned' ACLS

function usage() {
  echo "Usage: fix-acls.sh <csv file> <s3 bucket>"
  exit
}
[ -n "$1" ] || usage
[ -n "$2" ] || usage
csv_file="$1"
s3_bucket="$2"

IFS=","
grep -E '^Update ACL' "$csv_file" | while read -r  \
  action \
  video_key \
  lastmodified \
  canned_acl_expected \
  canned_acl \
  gloss_id \
  video_id \
  gloss_public \
  video_public \
  gloss_idgloss \
  gloss_created_at
do
  #echo $video_key $canned_acl_expected $canned_acl
  if [ "$canned_acl" != "$canned_acl_expected" ] ; then
    echo "$video_key $canned_acl --> $canned_acl_expected"
    aws s3api put-object-acl --acl "$canned_acl_expected" --bucket "$s3_bucket" --key "$video_key"
  fi
done
```

<br />

Example usage:

```
./fix-acls.sh  uat.csv  nzsl-signbank-media-example-uat
```

<br />

*End of document*







































