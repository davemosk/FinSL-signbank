# NZSL-Share - support for importing exported csvs from NZSL Share

NZSL Share supports exporting glosses in .csv format

NZSL Signbank aims to support the import of these .csv files, including
downloading media files from the NZSL Share server.

## NZSL-Share export file format (+notes)

The following .csv is exported from NZSL-Share:

| index | header | signbank field | notes |
| ---- | ---- | ---- | ---- |
| 1 | word | GlossTranslations.translations |  |
| 2 | maori | GlossTranslations.translations | comma-separated list |
| 3 | secondary | GlossTranslations.translations_secondary |  |
| 5 | notes | Comment.comment | assign to contributor |
| 6 | created_at | Gloss.created_at | To confirm if NZSL would like to store the value in the notes, it can't be set as the `created_at` field on the model |
| 7 | contributor_email | Comment.user_email |  |
| 8 | contributor_username | Comment.user_name |  |
| 9 | agrees |  | no current field - to be added with NZSL-77 |
| 10 | disagrees |  | no current field - to be added with NZSL-77 |
| 11 | topic_names | Gloss.semantic_fields | pipe-separated topics  |
| 12 | videos | GlossVideo.videofile | pipe-separated URLs |
| 13 | illustrations | GlossVideo.videofile | pipe-separated URLs |
| 14 | usage_examples | GlossVideo.videofile | pipe-separated URLs |
| 15 | sign_comments | Comment.user_name<br>Comment.comment | pipe-separated comments, in the format:<br><br>{username}:  {comment} |

## Signbank import process - objects to create

Per row in the .csv file, the following objects should be created.

### Gloss

One Gloss object:

| fieldname | from csv column | notes |
| ---- | ---- | ---- |
| dataset |  | will need to be set from upload context |
| published |  | default to False |
| exclude_from_ecv |  | check if this should be True or False |
| assigned_user |  | check if we should assign |
| idgloss | word | {word}:{gloss.pk}<br>may need to set temp value to save & get a pk |
| idgloss_mi | maori | {maori}:{gloss.pk}<br>may need to set temp value to save & get a pk |
| notes | notes, created_at | if NZSL wants to preserve the creation time it might need to be added to the notes |
| created_at |  | auto field in django |
| cretaed_by |  | check if we should set to the uploader |
| updated_by |  | set to the uploader |
| semantic_fields | topic_names | for each topic name:<br>if we have a semantic field with that name, add it otherwise, add the semantic field "miscellaneous"<br>ignore the topic "all signs" |

### GlossTranslation

Two GlossTranslation items:

#### English

| fieldname | from csv column | notes |
| ---- | ---- | ---- |
| gloss |  | the gloss object from above |
| language |  | English |
| translations | word |  |
| translations_secondary | secondary |  |

#### Maori

| fieldname | from csv column | notes |
| ---- | ---- | ---- |
| gloss |  | the gloss object from above |
| language |  | Maori |
| translations | maori | first of the maori words  if there are several |
| translations_secondary | maori | remaining maori words if there are several |

### Comment

Comments are provided by `django-contrib-comments`: [see docs](https://django-contrib-comments.readthedocs.io/en/latest/models.html)

#### Notes

one for the original uploader's notes:

| fieldname | from csv column | notes |
| ---- | ---- | ---- |
| content_object |  | generic foreign key pointing to the gloss |
| object_pk | gloss pk | PK value of the gloss |
| comment | notes |  |
| user_name | contributor_username |  |
| user_email | contributor_email |  |
| site |  | Required field, current Site object set in Django |
| is_public |  | set as False, only public comments are visible in Signbank |
| submit_date |  | Required field, date and time of import used |

#### Sign Comments

Then from other users - split `sign_comments` on pipe characters:

| fieldname | from csv column | notes |
| ---- | ---- | ---- |
| content_object |  | generic foreign should pointing to the gloss |
| comment | sign_comments | split on `:` and use item 1 |
| user_name | sign_comments | split on `:` and use item 0 |

### GlossVideo

There are three fields that are a pipe-separated array of video URLs:
- videos
- illustrations
- usage_examples

For each of these, we will need to download the file from the URL, and then the following should be stored in a GlossVideo object:

| fieldname | notes |
| ---- | ---- |
| gloss | the gloss we created for this csv row |
| videofile | the file we downloaded from nzsl-share |
| version | the order this file is displayed in the UI |
| is_public | False |

n.b.: the order requested is:
1. first video from `videos` (set `title` to `main`)
2. first video from `usage_examples` (set title to finalexample1)
3. second video from `usage_examples` (set title to finalexample2)

It is not clear what to do with:
- 2nd+ videos in the videos field
- 3rd+ videos in the usage_examples field
- ANY videos in the illustrations field

The screenshots provided show an illustration immediately after the first video, so we should confirm if we should follow that precedent.

## To confirm

- what is to be done for additional videos / images past those with outlined requirements
- is the user/owner to be added to the signer property
