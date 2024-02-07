# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs
import csv
import datetime
import random
import threading

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.contenttypes.models import ContentType
from django.contrib.sites.models import Site
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import HttpResponseRedirect
from django.shortcuts import render, reverse
from django.utils.timezone import get_current_timezone
from django.utils.translation import ugettext as _
from django_comments.models import Comment
from guardian.shortcuts import get_objects_for_user, get_perms
from tagging.models import Tag, TaggedItem

from .forms import CSVUploadForm
from .models import (Dataset, FieldChoice, Gloss,
                     GlossTranslations, Language,
                     ShareValidationAggregation)
from .tasks import retrieve_videos_for_glosses

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
                messages.add_message(request, messages.SUCCESS, _('Glosses were added succesfully.'))
            return render(request, "dictionary/import_gloss_csv_confirmation.html", {'glosses_added': glosses_added,
                                                                                     'dataset': dataset.name})
        else:
            return HttpResponseRedirect(reverse('dictionary:import_gloss_csv'))
    else:
        # If request method is not POST, redirect to the import form
        return HttpResponseRedirect(reverse('dictionary:import_gloss_csv'))


share_csv_header_list = [
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
        for row in glossreader:
            if glossreader.line_num == 1:
                continue
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
                  {"glosses_new": new_glosses,
                   "dataset": dataset, })


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

    elif "confirm" in request.POST:
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
            existing_machine_values = [mv for mv in
                                       FieldChoice.objects.all().values_list("machine_value",
                                                                             flat=True)]
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
                new_glosses[str(row_num)] = gloss_data
                bulk_create_gloss.append(Gloss(
                    dataset=dataset,
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
                if gloss_data.get("videos", None):
                    video_url = gloss_data["videos"]
                    extension = video_url[-3:]
                    file_name = (
                        f"{gloss.pk}-{gloss.idgloss}_video.{extension}"
                    )

                    glossvideo = {
                        "url": video_url,
                        "file_name": file_name,
                        "gloss_pk": gloss.pk,
                        "title": "Main",
                        "version": 0
                    }
                    videos.append(glossvideo)

                if gloss_data.get("illustrations", None):
                    for i, video_url in enumerate(gloss_data["illustrations"].split("|")):
                        extension = video_url[-3:]
                        file_name = (
                            f"{gloss.pk}-{gloss.idgloss}_illustration_{i + 1}.{extension}"
                        )

                        glossvideo = {
                            "url": video_url,
                            "file_name": file_name,
                            "gloss_pk": gloss.pk,
                            "title": "Illustration",
                            "version": i
                        }
                        videos.append(glossvideo)

                if gloss_data.get("usage_examples", None):
                    for i, video_url in enumerate(gloss_data["usage_examples"].split("|")):
                        extension = video_url[-3:]
                        file_name = (
                            f"{gloss.pk}-{gloss.idgloss}_usageexample_{i + 1}.{extension}"
                        )

                        glossvideo = {
                            "url": video_url,
                            "file_name": file_name,
                            "gloss_pk": gloss.pk,
                            "title": f"finalexample{i + 1}",
                            "version": i
                        }
                        videos.append(glossvideo)

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
            messages.add_message(request, messages.SUCCESS, _("Glosses were added succesfully."))
        return render(
            request, "dictionary/import_nzsl_share_gloss_csv_confirmation.html",
            {"glosses_added": glosses_added, "dataset": dataset.name}
        )
    else:
        return HttpResponseRedirect(reverse("dictionary:import_nzsl_share_gloss_csv"))


