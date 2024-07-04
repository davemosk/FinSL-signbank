# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs
import csv
import datetime
import random
import re
import threading

from _collections import defaultdict
from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.contenttypes.models import ContentType
from django.contrib.sites.models import Site
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.http import HttpResponseRedirect
from django.shortcuts import render, reverse
from django.utils.timezone import get_current_timezone
from django.utils.translation import gettext as _
from django_comments.models import Comment
from guardian.shortcuts import get_objects_for_user, get_perms
from tagging.models import Tag, TaggedItem

from .forms import CSVFileOnlyUpload, CSVUploadForm
from .models import (Dataset, FieldChoice, Gloss, GlossTranslations, Language,
                     ManualValidationAggregation, ShareValidationAggregation, ValidationRecord)
from .tasks import retrieve_videos_for_glosses
from ..video.models import GlossVideo

User = get_user_model()


@login_required
@permission_required('dictionary.import_csv')
def import_gloss_csv(request):
    """
    Check which objects exist and which not. Then show the user a list of glosses that will be added if user confirms.
    Store the glosses to be added into sessions.
    """
    glosses_new = []
    glosses_exists = []
    # Make sure that the session variables are flushed before using this view.
    if 'dataset_id' in request.session: del request.session['dataset_id']
    if 'glosses_new' in request.session: del request.session['glosses_new']

    if request.method == 'POST':
        form = CSVUploadForm(request.POST, request.FILES)
        if form.is_valid():
            dataset = form.cleaned_data['dataset']
            if 'view_dataset' not in get_perms(request.user, dataset):
                # If user has no permissions to dataset, raise PermissionDenied to show 403 template.
                msg = _("You do not have permissions to import glosses to this lexicon.")
                messages.error(request, msg)
                raise PermissionDenied(msg)
            try:
                glossreader = csv.reader(codecs.iterdecode(form.cleaned_data['file'], 'utf-8'), delimiter=',', quotechar='"')
            except csv.Error as e:
                # Can't open file, remove session variables
                if 'dataset_id' in request.session: del request.session['dataset_id']
                if 'glosses_new' in request.session: del request.session['glosses_new']
                # Set a message to be shown so that the user knows what is going on.
                messages.add_message(request, messages.ERROR, _('Cannot open the file:' + str(e)))
                return render(request, 'dictionary/import_gloss_csv.html', {'import_csv_form': CSVUploadForm()}, )
            except UnicodeDecodeError as e:
                # File is not UTF-8 encoded.
                messages.add_message(request, messages.ERROR, _('File must be UTF-8 encoded!'))
                return render(request, 'dictionary/import_gloss_csv.html', {'import_csv_form': CSVUploadForm()}, )

            for row in glossreader:
                if glossreader.line_num == 1:
                    # Skip first line of CSV file.
                    continue
                try:
                    # Find out if the gloss already exists, if it does add to list of glosses not to be added.
                    gloss = Gloss.objects.get(dataset=dataset, idgloss=row[0])
                    glosses_exists.append(gloss)
                except Gloss.DoesNotExist:
                    # If gloss is not already in list, add glossdata to list of glosses to be added as a tuple.
                    if not any(row[0] in s for s in glosses_new):
                        glosses_new.append(tuple(row))
                except IndexError:
                    # If row[0] does not exist, continue to next iteration of loop.
                    continue

            # Store dataset's id and the list of glosses to be added in session.
            request.session['dataset_id'] = dataset.id
            request.session['glosses_new'] = glosses_new

            return render(request, 'dictionary/import_gloss_csv_confirmation.html',
                          {'glosses_new': glosses_new,
                           'glosses_exists': glosses_exists,
                           'dataset': dataset, })
        else:
            # If form is not valid, set a error message and return to the original form.
            messages.add_message(request, messages.ERROR, _('The provided CSV-file does not meet the requirements '
                                                            'or there is some other problem.'))
            return render(request, 'dictionary/import_gloss_csv.html', {'import_csv_form': form}, )
    else:
        # If request type is not POST, return to the original form.
        csv_form = CSVUploadForm()
        allowed_datasets = get_objects_for_user(request.user, 'dictionary.view_dataset')
        # Make sure we only list datasets the user has permissions to.
        csv_form.fields["dataset"].queryset = csv_form.fields["dataset"].queryset.filter(
            id__in=[x.id for x in allowed_datasets])
        return render(request, "dictionary/import_gloss_csv.html",
                      {'import_csv_form': csv_form}, )


@login_required
@permission_required('dictionary.import_csv')
def confirm_import_gloss_csv(request):
    """This view adds the data to database if the user confirms the action"""
    if request.method == 'POST':
        if 'cancel' in request.POST:
            # If user cancels adding data, flush session variables
            if 'dataset_id' in request.session: del request.session['dataset_id']
            if 'glosses_new' in request.session: del request.session['glosses_new']
            # Set a message to be shown so that the user knows what is going on.
            messages.add_message(request, messages.WARNING, _('Cancelled adding CSV data.'))
            return HttpResponseRedirect(reverse('dictionary:import_gloss_csv'))

        elif 'confirm' in request.POST:
            glosses_added = []
            dataset = None
            if 'glosses_new' and 'dataset_id' in request.session:
                dataset = Dataset.objects.get(id=request.session['dataset_id'])
                for gloss in request.session['glosses_new']:

                    # If the Gloss does not already exist, continue adding.
                    if not Gloss.objects.filter(dataset=dataset, idgloss=gloss[0]).exists():
                        try:
                            new_gloss = Gloss(dataset=dataset, idgloss=gloss[0], idgloss_mi=gloss[1],
                                          created_by=request.user, updated_by=request.user)
                        except IndexError:
                            # If we get IndexError, idgloss_mi was probably not provided
                            new_gloss = Gloss(dataset=dataset, idgloss=gloss[0],
                                              created_by=request.user, updated_by=request.user)

                        new_gloss.save()
                        glosses_added.append((new_gloss.idgloss, new_gloss.idgloss_mi))

                # Flush request.session['glosses_new'] and request.session['dataset']
                del request.session['glosses_new']
                del request.session['dataset_id']
                # Set a message to be shown so that the user knows what is going on.
                messages.add_message(request, messages.SUCCESS, _('Glosses were added successfully.'))
            return render(request, "dictionary/import_gloss_csv_confirmation.html", {'glosses_added': glosses_added,
                                                                                     'dataset': dataset.name})
        else:
            return HttpResponseRedirect(reverse('dictionary:import_gloss_csv'))
    else:
        # If request method is not POST, redirect to the import form
        return HttpResponseRedirect(reverse('dictionary:import_gloss_csv'))


share_csv_header_list = [
    "id",
    "word",
    "maori",
    "secondary",
    "notes",
    "created_at",
    "contributor_email",
    "contributor_username",
    "agrees",
    "disagrees",
    "topic_names",
    "videos",
    "illustrations",
    "usage_examples",
    "sign_comments",
]


@login_required
@permission_required("dictionary.import_csv")
def import_nzsl_share_gloss_csv(request):
    """
    Import a file containing glosses from NZSL Share.
    """
    # Make sure that the session variables are flushed before using this view.
    request.session.pop("dataset_id", None)
    request.session.pop("glosses_new", None)

    if not request.method == "POST":
        # If request type is not POST, return to the original form.
        csv_form = CSVUploadForm()
        allowed_datasets = get_objects_for_user(request.user, "dictionary.view_dataset")
        # Make sure we only list datasets the user has permissions to.
        csv_form.fields["dataset"].queryset = csv_form.fields["dataset"].queryset.filter(
            id__in=[x.id for x in allowed_datasets])
        return render(request, "dictionary/import_nzsl_share_gloss_csv.html",
                      {"import_csv_form": csv_form}, )

    form = CSVUploadForm(request.POST, request.FILES)

    if not form.is_valid():
        # If form is not valid, set a error message and return to the original form.
        messages.add_message(request, messages.ERROR,
                             _("The provided CSV-file does not meet the requirements "
                               "or there is some other problem."))
        return render(request, "dictionary/import_nzsl_share_gloss_csv.html",
                      {"import_csv_form": form}, )

    new_glosses = []
    dataset = form.cleaned_data["dataset"]
    if "view_dataset" not in get_perms(request.user, dataset):
        # If user has no permissions to dataset, raise PermissionDenied to show 403 template.
        msg = _("You do not have permissions to import glosses to this lexicon.")
        messages.error(request, msg)
        raise PermissionDenied(msg)
    try:
        glossreader = csv.DictReader(
            codecs.iterdecode(form.cleaned_data["file"], "utf-8"),
            fieldnames=share_csv_header_list,
            delimiter=",",
            quotechar='"'
        )

        skipped_existing_glosses = []
        existing_nzsl_share_ids = set(
            Gloss.objects.exclude(nzsl_share_id__exact="").values_list(
                "nzsl_share_id", flat=True
            )
        )

        for row in glossreader:
            if glossreader.line_num == 1:
                continue
            if row["id"] in existing_nzsl_share_ids:

                # nzsl_share_id is not a reliable index, due to manual intervention
                try:
                    gloss = Gloss.objects.filter(nzsl_share_id=row["id"]).get()
                except (Gloss.DoesNotExist, Gloss.MultipleObjectsReturned) as e:
                    print(f"nzsl_share_id = {row['id']} {str(e)}")
                    skipped_existing_glosses.append(row)
                    continue

                # if gloss has video/s we skip it, otherwise we add it anyway
                if gloss.glossvideo_set.all().exists():
                    skipped_existing_glosses.append(row)
                else:
                    new_glosses.append(row)
            else:
                new_glosses.append(row)

    except csv.Error as e:
        # Can't open file, remove session variables
        request.session.pop("dataset_id", None)
        request.session.pop("glosses_new", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.ERROR, _("Cannot open the file:" + str(e)))
        return render(request, "dictionary/import_nzsl_share_gloss_csv.html",
                      {"import_csv_form": CSVUploadForm()}, )
    except UnicodeDecodeError as e:
        # File is not UTF-8 encoded.
        messages.add_message(request, messages.ERROR, _("File must be UTF-8 encoded!"))
        return render(request, "dictionary/import_nzsl_share_gloss_csv.html",
                      {"import_csv_form": CSVUploadForm()}, )

    # Store dataset's id and the list of glosses to be added in session.
    request.session["dataset_id"] = dataset.id
    request.session["glosses_new"] = new_glosses

    return render(request, "dictionary/import_nzsl_share_gloss_csv_confirmation.html",
                  {
                      "glosses_new": new_glosses,
                      "dataset": dataset,
                      "skipped_existing_glosses": skipped_existing_glosses
                  })


def update_retrieval_videos(videos, gloss_data):
    """ prep videos, illustrations and usage example for video retrieval """

    gloss_pk = gloss_data["gloss"].pk
    gloss_word = gloss_data["word"]

    if gloss_data.get("videos", None):
        video_url = gloss_data["videos"]
        extension = video_url[-3:]
        file_name = (
            f"{gloss_pk}-{gloss_word}.{gloss_pk}_video.{extension}"
        )

        glossvideo = {
            "url": video_url,
            "file_name": file_name,
            "gloss_pk": gloss_pk,
            "video_type": "main",
            "version": 0
        }
        videos.append(glossvideo)

    if gloss_data.get("illustrations", None):
        for i, video_url in enumerate(gloss_data["illustrations"].split("|")):
            extension = video_url[-3:]
            file_name = (
                f"{gloss_pk}-{gloss_word}.{gloss_pk}_illustration_{i + 1}.{extension}"
            )

            glossvideo = {
                "url": video_url,
                "file_name": file_name,
                "gloss_pk": gloss_pk,
                "video_type": "main",
                "version": i
            }
            videos.append(glossvideo)

    if gloss_data.get("usage_examples", None):
        for i, video_url in enumerate(gloss_data["usage_examples"].split("|")):
            extension = video_url[-3:]
            file_name = (
                f"{gloss_pk}-{gloss_word}.{gloss_pk}_usageexample_{i + 1}.{extension}"
            )

            glossvideo = {
                "url": video_url,
                "file_name": file_name,
                "gloss_pk": gloss_pk,
                "video_type": f"finalexample{i + 1}",
                "version": i
            }
            videos.append(glossvideo)

@login_required
@permission_required("dictionary.import_csv")
@transaction.atomic()
def confirm_import_nzsl_share_gloss_csv(request):
    """This view adds the data to database if the user confirms the action"""
    if not request.method == "POST":
        # If request method is not POST, redirect to the import form
        return HttpResponseRedirect(reverse("dictionary:import_nzsl_share_gloss_csv"))

    if "cancel" in request.POST:
        # If user cancels adding data, flush session variables
        request.session.pop("dataset_id", None)
        request.session.pop("glosses_new", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.WARNING, _("Cancelled adding CSV data."))
        return HttpResponseRedirect(reverse("dictionary:import_nzsl_share_gloss_csv"))
    elif not "confirm" in request.POST:
        return HttpResponseRedirect(reverse("dictionary:import_nzsl_share_gloss_csv"))

    glosses_added = []
    dataset = None
    translations = []
    comments = []
    videos = []
    new_glosses = {}
    bulk_create_gloss = []
    bulk_update_glosses = []
    bulk_semantic_fields = []
    bulk_tagged_items = []
    contributors = []
    bulk_share_validation_aggregations = []
    video_import_only_glosses_data = []

    if "glosses_new" and "dataset_id" in request.session:
        dataset = Dataset.objects.get(id=request.session["dataset_id"])
        language_en = Language.objects.get(name="English")
        language_mi = Language.objects.get(name="Māori")
        gloss_content_type = ContentType.objects.get_for_model(Gloss)
        site = Site.objects.get_current()
        comment_submit_date = datetime.datetime.now(tz=get_current_timezone())
        semantic_fields = FieldChoice.objects.filter(
            field="semantic_field"
        ).values_list("english_name", "pk")
        semantic_fields_dict = {field[0]: field[1] for field in semantic_fields}
        signers = FieldChoice.objects.filter(field="signer")
        signer_dict = {signer.english_name: signer for signer in signers}
        existing_machine_values = [
            mv for mv in FieldChoice.objects.all().values_list("machine_value", flat=True)
        ]
        not_public_tag = Tag.objects.get(name="not public")
        nzsl_share_tag = Tag.objects.get(name="nzsl-share")
        import_user = User.objects.get(
            username="nzsl_share_importer",
            first_name="Importer",
            last_name="NZSL Share",
        )

        for row_num, gloss_data in enumerate(request.session["glosses_new"]):
            # will iterate over these glosses again after bulk creating
            # and to ensure we get the correct gloss_data for words that appear multiple
            # times we'll use the row_num as the identifier for the gloss data

            # if the gloss already exists at this point, it can only mean that
            # it has no videos and we want to import videos for it
            # try-except saves us a db call
            try:
                gloss = Gloss.objects.filter(nzsl_share_id=gloss_data["id"]).get()
                gloss_data_copy = gloss_data.copy()
                gloss_data_copy["gloss"] = gloss
                video_import_only_glosses_data.append(gloss_data_copy)
                continue
            except Gloss.DoesNotExist:
                pass

            new_glosses[str(row_num)] = gloss_data
            bulk_create_gloss.append(Gloss(
                dataset=dataset,
                nzsl_share_id=gloss_data["id"],
                # need to make idgloss unique in dataset,
                # but gloss word can appear in multiple rows, so
                # idgloss will be updated to word:pk in second step
                idgloss=f"{gloss_data['word']}_row{row_num}",
                idgloss_mi=gloss_data.get("maori", None),
                created_by=import_user,
                updated_by=import_user,
                exclude_from_ecv=True,
            ))
            contributors.append(gloss_data["contributor_username"])

        bulk_created = Gloss.objects.bulk_create(bulk_create_gloss)

        # Create new signers for contributors that do not exist as signers yet
        contributors = set(contributors)
        create_signers = []
        signers = signer_dict.keys()
        for contributor in contributors:
            if contributor not in signers:
                new_machine_value = random.randint(0, 99999999)
                while new_machine_value in existing_machine_values:
                    new_machine_value = random.randint(0, 99999999)
                existing_machine_values.append(new_machine_value)
                create_signers.append(FieldChoice(
                    field="signer",
                    english_name=contributor,
                    machine_value=new_machine_value
                ))
        new_signers = FieldChoice.objects.bulk_create(create_signers)
        for signer in new_signers:
            signer_dict[signer.english_name] = signer

        for gloss in bulk_created:
            word_en, row_num = gloss.idgloss.split("_row")
            gloss_data = new_glosses[row_num]
            gloss_data["gloss"] = gloss

            # get semantic fields for gloss_data topics
            if gloss_data.get("topic_names", None):
                gloss_topics = gloss_data["topic_names"].split("|")
                # ignore all signs and All signs
                cleaned_gloss_topics = [
                    x for x in gloss_topics if x not in ["all signs", "All signs"]
                ]
                add_miscellaneous = False

                for topic in cleaned_gloss_topics:
                    if topic in semantic_fields_dict.keys():
                        bulk_semantic_fields.append(
                            Gloss.semantic_field.through(
                                gloss_id=gloss.id,
                                fieldchoice_id=semantic_fields_dict[topic]
                            )
                        )
                    else:
                        # add the miscellaneous semantic field if a topic does not exist
                        add_miscellaneous = True

                if add_miscellaneous:
                    bulk_semantic_fields.append(
                        Gloss.semantic_field.through(
                            gloss_id=gloss.id,
                            fieldchoice_id=semantic_fields_dict["Miscellaneous"]
                        )
                    )

            # create GlossTranslations for english and maori words
            translations.append(GlossTranslations(
                gloss=gloss,
                language=language_en,
                translations=gloss_data["word"],
                translations_secondary=gloss_data.get("secondary", None)
            ))
            if gloss_data.get("maori", None):
                # There is potentially several comma separated maori words
                maori_words = gloss_data["maori"].split(", ")

                # Update idgloss_mi using first maori word, then create translation
                gloss.idgloss_mi = f"{maori_words[0]}:{gloss.pk}"

                translation = GlossTranslations(
                    gloss=gloss,
                    language=language_mi,
                    translations=maori_words[0]
                )
                if len(maori_words) > 1:
                    translation.translations_secondary = ", ".join(maori_words[1:])

                translations.append(translation)

            # Prepare new idgloss and signer fields for bulk update
            gloss.idgloss = f"{word_en}:{gloss.pk}"
            gloss.signer = signer_dict[gloss_data["contributor_username"]]
            bulk_update_glosses.append(gloss)

            # Create comment for gloss_data notes
            comments.append(Comment(
                content_type=gloss_content_type,
                object_pk=gloss.pk,
                user_name=gloss_data.get("contributor_username", ""),
                comment=gloss_data.get("notes", ""),
                site=site,
                is_public=False,
                submit_date=comment_submit_date
            ))
            if gloss_data.get("sign_comments", None):
                # create Comments for all gloss_data sign_comments
                for comment in gloss_data["sign_comments"].split("|"):
                    try:
                        comment_content = comment.split(":")
                        user_name = comment_content[0]
                        comment_content = comment_content[1]
                    except IndexError:
                        comment_content = comment
                        user_name = "Unknown"
                    comments.append(Comment(
                        content_type=gloss_content_type,
                        object_pk=gloss.pk,
                        user_name=user_name,
                        comment=comment_content,
                        site=site,
                        is_public=False,
                        submit_date=comment_submit_date
                    ))

            # Add ShareValidationAggregation
            bulk_share_validation_aggregations.append(ShareValidationAggregation(
                gloss=gloss,
                agrees=int(gloss_data["agrees"]),
                disagrees=int(gloss_data["disagrees"])
            ))

            # prep videos, illustrations and usage example for video retrieval
            update_retrieval_videos(videos, gloss_data)

            glosses_added.append(gloss)

            bulk_tagged_items.append(TaggedItem(
                content_type=gloss_content_type,
                object_id=gloss.pk,
                tag=nzsl_share_tag

            ))
            bulk_tagged_items.append(TaggedItem(
                content_type=gloss_content_type,
                object_id=gloss.pk,
                tag=not_public_tag

            ))

        # Bulk create entities related to the gloss, and bulk update the glosses' idgloss
        Comment.objects.bulk_create(comments)
        GlossTranslations.objects.bulk_create(translations)
        Gloss.objects.bulk_update(bulk_update_glosses, ["idgloss", "idgloss_mi", "signer"])
        Gloss.semantic_field.through.objects.bulk_create(bulk_semantic_fields)
        TaggedItem.objects.bulk_create(bulk_tagged_items)
        ShareValidationAggregation.objects.bulk_create(bulk_share_validation_aggregations)

        # Add the video-update only glosses
        for video_import_gloss_data in video_import_only_glosses_data:
            # prep videos, illustrations and usage example for video retrieval
            update_retrieval_videos(videos, video_import_gloss_data)
            glosses_added.append(video_import_gloss_data["gloss"])

        # start Thread to process gloss video retrieval in the background
        t = threading.Thread(
            target=retrieve_videos_for_glosses,
            args=[videos],
            daemon=True
        )
        t.start()

        del request.session["glosses_new"]
        del request.session["dataset_id"]

        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.SUCCESS, _("Glosses were added successfully."))
    return render(
        request, "dictionary/import_nzsl_share_gloss_csv_confirmation.html",
        {
            "glosses_added": glosses_added,
            "dataset": dataset.name
        }
    )



@login_required
@permission_required("dictionary.import_csv")
def import_qualtrics_csv(request):
    """
    Import ValidationRecords from a CSV export from Qualtrics
    """
    # Make sure that the session variables are flushed before using this view.
    request.session.pop("validation_records", None)
    request.session.pop("question_numbers", None)
    request.session.pop("question_gloss_map", None)

    if not request.method == "POST":
        # If request type is not POST, return to the original form.
        csv_form = CSVFileOnlyUpload()
        return render(request, "dictionary/import_qualtrics_csv.html",
                      {"import_csv_form": csv_form}, )

    form = CSVFileOnlyUpload(request.POST, request.FILES)

    if not form.is_valid():
        # If form is not valid, set a error message and return to the original form.
        messages.add_message(request, messages.ERROR,
                             _("The provided CSV-file does not meet the requirements "
                               "or there is some other problem."))
        return render(request, "dictionary/import_qualtrics_csv.html",
                      {"import_csv_form": form}, )

    validation_records = []
    skipped_rows = []
    try:
        validation_record_reader = csv.DictReader(
            codecs.iterdecode(form.cleaned_data["file"], "utf-8"),
            delimiter=",",
            quotechar='"'
        )

        question_numbers = []
        question_to_glossvideo_map = {}

        for header in validation_record_reader.fieldnames:
            # The format of the three question headers pertaining to each gloss is described in
            # under docs/validation_result_model
            # We are using the first question column {question_number}_Q1_1 to identify the
            # question number
            question_match = re.search("(\d+\_Q1\_1)", header)
            if question_match:
                question_number = question_match[0].split("_Q1_1")[0]
                question_numbers.append(question_number)

        for row in validation_record_reader:
            # Qualtrics validation record csv has 3 rows before actual records start
            # skipping row 1 and 3, row 2 contains the gloss video url
            if validation_record_reader.line_num in (1, 3):
                continue
            elif validation_record_reader.line_num == 2:
                # Extract gloss pks from urls for each question number from the second line
                # The second line is build something like {number}_Q1 - {video_url} - Have seen it or use it myself
                # and each url is build as bellow:
                # {host}/video/signed_url/{token}/{video pk}/
                # See docs/validation_result_model for more info
                for question in question_numbers:
                    video_pk = row[f"{question}_Q1_1"].split("/")[-2]
                    question_to_glossvideo_map[question] = int(video_pk)

            elif row["Status"] not in ("IP Address", "Imported"):
                skipped_rows.append(row)
            else:
                validation_records.append(row)

    except csv.Error as e:
        # Can't open file, remove session variables
        request.session.pop("validation_records", None)
        request.session.pop("question_numbers", None)
        request.session.pop("question_gloss_map", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.ERROR, _("Cannot open the file:" + str(e)))
        return render(request, "dictionary/import_qualtrics_csv.html",
                      {"import_csv_form": CSVFileOnlyUpload()}, )
    except UnicodeDecodeError as e:
        # File is not UTF-8 encoded.
        messages.add_message(request, messages.ERROR, _("File must be UTF-8 encoded!"))
        return render(request, "dictionary/import_qualtrics_csv.html",
                      {"import_csv_form": CSVFileOnlyUpload()}, )

    # Store dataset's id and the list of glosses to be added in session.
    request.session["validation_records"] = validation_records
    request.session["question_numbers"] = question_numbers
    request.session["question_glossvideo_map"] = question_to_glossvideo_map

    return render(request, "dictionary/import_qualtrics_csv_confirmation.html",
                  {"validation_records": validation_records, "skipped_rows": skipped_rows})


@login_required
@permission_required("dictionary.import_csv")
@transaction.atomic()
def confirm_import_qualtrics_csv(request):
    """This view adds the data to database if the user confirms the action"""
    if not request.method == "POST":
        # If request method is not POST, redirect to the import form
        return HttpResponseRedirect(reverse("dictionary:import_qualtrics_csv"))

    if "cancel" in request.POST:
        # If user cancels adding data, flush session variables
        request.session.pop("validation_records", None)
        request.session.pop("question_numbers", None)
        request.session.pop("question_glossvideo_map", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.WARNING, _("Cancelled adding CSV data."))
        return HttpResponseRedirect(reverse("dictionary:import_qualtrics_csv"))

    if not "confirm" in request.POST:
        return HttpResponseRedirect(reverse("dictionary:import_qualtrics_csv"))

    validation_records_added = []
    validation_records = []
    missing_gloss_pk_question_pairs = {}
    bulk_tagged_items = []
    gloss_pks = set()

    if "validation_records" and "question_numbers" and "question_glossvideo_map" in request.session:
        # Retrieve glosses
        glossvideo_pk_list = request.session["question_glossvideo_map"].values()
        glossvideo_dict = GlossVideo.objects.select_related("gloss").in_bulk(glossvideo_pk_list)
        gloss_content_type = ContentType.objects.get_for_model(Gloss)
        check_result_tag = Tag.objects.get(name=settings.TAG_VALIDATION_CHECK_RESULTS)
        ready_for_validation_tag = Tag.objects.get(name=settings.TAG_READY_FOR_VALIDATION)

        questions_numbers = request.session["question_numbers"]
        question_glossvideo_map = request.session["question_glossvideo_map"]
        validation_records = request.session["validation_records"]

        # Go through csv data
        for record in validation_records:
            response_id = record.get("ResponseId", "")
            respondent_first_name = record.get("RecipientFirstName", "")
            respondent_last_name = record.get("RecipientLastName", "")

            for question_number in questions_numbers:
                sign_seen = (record[f"{question_number}_Q1_1"]).lower()
                # the not sure response has spaces, so we're replacing with the value of the
                # SignSeenChoice on the model
                if sign_seen == "not sure ":
                    sign_seen = ValidationRecord.SignSeenChoices.NOT_SURE.value

                try:
                    gloss = glossvideo_dict[question_glossvideo_map[question_number]].gloss
                    validation_records_added.append(ValidationRecord(
                        gloss=gloss,
                        sign_seen=ValidationRecord.SignSeenChoices(sign_seen),
                        response_id=response_id,
                        respondent_first_name=respondent_first_name,
                        respondent_last_name=respondent_last_name,
                        comment=record.get(f"{question_number}_Q2_5_TEXT", ""),
                    ))
                    gloss_pks.add(gloss.pk)
                except KeyError:
                    missing_gloss_pk_question_pairs[question_number] = question_glossvideo_map[
                        question_number]

        for gloss_pk in gloss_pks:
            bulk_tagged_items.append(TaggedItem(
                content_type=gloss_content_type,
                object_id=gloss_pk,
                tag=check_result_tag

            ))

        # ignoring conflicts so the unique together on the model filters out potential duplicates
        ValidationRecord.objects.bulk_create(validation_records_added, ignore_conflicts=True)
        TaggedItem.objects.bulk_create(bulk_tagged_items, ignore_conflicts=True)
        TaggedItem.objects.filter(
            content_type=gloss_content_type,
            object_id__in=gloss_pks,
            tag=ready_for_validation_tag
        ).delete()

        del request.session["validation_records"]
        del request.session["question_numbers"]
        del request.session["question_glossvideo_map"]

        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.SUCCESS,
                             _("ValidationRecords were added successfully."))
    return render(
        request, "dictionary/import_qualtrics_csv_confirmation.html",
        {
            "validation_records_added": validation_records_added,
            "validation_record_count": len(validation_records_added),
            "responses_count": len(validation_records),
            "gloss_count": len(gloss_pks),
            "missing_gloss_question_pairs": missing_gloss_pk_question_pairs
        }
    )


def _check_row_can_be_converted_to_integer(row, keys):
    for key in keys:
        if row[key]:
            try:
                int(row[key])
            except ValueError:
                raise ValidationError(
                    f"Row for group {row['group']} - gloss {row['idgloss']} contains non-integer "
                    f"{key.upper()} column value"
                )


@login_required
@permission_required("dictionary.import_csv")
def import_manual_validation(request):
    """
    Import ManualValidationAggregations from a CSV file
    """
    # Make sure that the session variables are flushed before using this view.
    request.session.pop("group_row_map", None)
    request.session.pop("glosses", None)

    if request.method != "POST":
        # If request type is not POST, return to the original form.
        csv_form = CSVFileOnlyUpload()
        return render(request, "dictionary/import_manual_validation_csv.html",
                      {"import_csv_form": csv_form}, )

    form = CSVFileOnlyUpload(request.POST, request.FILES)

    if not form.is_valid():
        # If form is not valid, set a error message and return to the original form.
        messages.add_message(request, messages.ERROR,
                             _("The provided CSV-file does not meet the requirements "
                               "or there is some other problem."))
        return render(request, "dictionary/import_manual_validation_csv.html",
                      {"import_csv_form": form}, )

    group_row_map = defaultdict(list)
    group_gloss_count = defaultdict(int)
    glosses = []
    required_headers = [
        "group",
        "idgloss",
        "yes",
        "no",
        "abstain or not sure",
        "comments"
    ]
    try:
        validation_record_reader = csv.DictReader(
            codecs.iterdecode(form.cleaned_data["file"], "utf-8-sig"),
            delimiter=",",
            quotechar='"'
        )
        missing_headers = set(required_headers) - set(validation_record_reader.fieldnames)
        if missing_headers != set():
            request.session.pop("group_row_map", None)
            request.session.pop("glosses", None)
            # Set a message to be shown so that the user knows what is going on.
            messages.add_message(request, messages.ERROR,
                                 _(f"CSV is missing required columns: {missing_headers}"))
            return render(request,
                              "dictionary/import_manual_validation_csv.html",
                              {"import_csv_form": CSVFileOnlyUpload()}, )

        for row in validation_record_reader:
            if validation_record_reader.line_num == 1:
                continue
            _check_row_can_be_converted_to_integer(row, ["yes", "no", "abstain or not sure"])
            group_row_map[row["group"]].append(row)
            group_gloss_count[row["group"]] += 1
            glosses.append(row["idgloss"].split(":")[1])

    except ValidationError as e:
        request.session.pop("group_row_map", None)
        request.session.pop("glosses", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.ERROR, _("File contains non-compliant data:" + str(e)))
        return render(request, "dictionary/import_manual_validation_csv.html",
                      {"import_csv_form": CSVFileOnlyUpload()}, )

    except csv.Error as e:
        # Can't open file, remove session variables
        request.session.pop("group_row_map", None)
        request.session.pop("glosses", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.ERROR, _("Cannot open the file:" + str(e)))
        return render(request, "dictionary/import_manual_validation_csv.html",
                      {"import_csv_form": CSVFileOnlyUpload()}, )
    except UnicodeDecodeError as e:
        # File is not UTF-8 encoded.
        messages.add_message(request, messages.ERROR, _("File must be UTF-8 encoded!"))
        return render(request, "dictionary/import_manual_validation_csv.html",
                      {"import_csv_form": CSVFileOnlyUpload()}, )

    # Store dataset's id and the list of glosses to be added in session.
    request.session["group_row_map"] = group_row_map
    request.session["glosses"] = list(set(glosses))

    return render(
        request, "dictionary/import_manual_validation_csv_confirmation.html",
        {
            # iterating over defaultdicts causes issues in template rendering
            "group_row_map": dict(group_row_map),
            "group_gloss_count": dict(group_gloss_count)
        }
    )


@login_required
@permission_required("dictionary.import_csv")
@transaction.atomic()
def confirm_import_manual_validation(request):
    """This view adds the data to database if the user confirms the action"""
    if not request.method == "POST":
        # If request method is not POST, redirect to the import form
        return HttpResponseRedirect(reverse("dictionary:import_manual_validation_csv"))

    if "cancel" in request.POST:
        # If user cancels adding data, flush session variables
        request.session.pop("group_row_map", None)
        request.session.pop("glosses", None)
        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.WARNING, _("Cancelled adding CSV data."))
        return HttpResponseRedirect(reverse("dictionary:import_manual_validation_csv"))

    if not "confirm" in request.POST:
        return HttpResponseRedirect(reverse("dictionary:import_manual_validation_csv"))

    manual_validation_aggregations = []
    missing_glosses = []

    if "group_row_map" and "glosses" in request.session:
        gloss_pk_list = request.session["glosses"]
        gloss_dict = Gloss.objects.in_bulk(gloss_pk_list)

        gloss_row_map = request.session["group_row_map"]

        # Go through csv data
        for group, rows in gloss_row_map.items():
            for row in rows:
                gloss = gloss_dict.get(int(row["idgloss"].split(":")[1]))
                if not gloss:
                    missing_glosses.append((group, row["idgloss"]))
                    continue
                sign_seen_yes = row["yes"]
                sign_seen_no = row["no"]
                sign_seen_not_sure = row["abstain or not sure"]
                comments = row["comments"]
                manual_validation_aggregations.append(ManualValidationAggregation(
                    gloss=gloss,
                    group=group,
                    sign_seen_yes=int(sign_seen_yes) if sign_seen_yes else 0,
                    sign_seen_no=int(sign_seen_no) if sign_seen_no else 0,
                    sign_seen_not_sure=int(sign_seen_not_sure) if sign_seen_not_sure else 0,
                    comments=comments
                ))

        ManualValidationAggregation.objects.bulk_create(manual_validation_aggregations)

        del request.session["group_row_map"]
        del request.session["glosses"]

        # Set a message to be shown so that the user knows what is going on.
        messages.add_message(request, messages.SUCCESS,
                             _("ValidationRecords were added successfully."))
    return render(
        request, "dictionary/import_manual_validation_csv_confirmation.html",
        {
            "manual_validation_aggregations": manual_validation_aggregations,
            "manual_validation_aggregations_count": len(manual_validation_aggregations),
            "missing_glosses": missing_glosses
        }
    )
