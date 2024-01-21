# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import codecs
import csv
import datetime
import re

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.contenttypes.models import ContentType
from django.contrib.sites.models import Site
from django.core.exceptions import ObjectDoesNotExist, PermissionDenied
from django.core.management import call_command
from django.db.models.fields import BooleanField
from django.db import transaction
from django.http import (Http404, HttpResponse, HttpResponseBadRequest,
                         HttpResponseForbidden, HttpResponseNotAllowed,
                         HttpResponseRedirect, HttpResponseServerError)
from django.shortcuts import get_object_or_404, redirect, render, reverse
from django.utils.translation import ugettext as _
from django.utils.timezone import get_current_timezone
from django_comments.models import Comment
from guardian.shortcuts import get_objects_for_user, get_perms
from urllib.request import urlretrieve

from tagging.models import Tag, TaggedItem


from ..video.models import GlossVideo
from .forms import (CSVUploadForm, GlossRelationForm, MorphologyForm,
                    RelationForm, RelationToForeignSignForm, TagDeleteForm,
                    TagsAddForm, TagUpdateForm)
from .models import (Dataset, Dialect, FieldChoice, Gloss, Lemma, GlossRelation,
                     GlossTranslations, GlossURL, Keyword, Language,
                     MorphologyDefinition, Relation, RelationToForeignSign,
                     Translation, build_choice_list)
from .tasks import retrieve_videos_for_glosses


@permission_required('dictionary.change_gloss')
def update_gloss(request, glossid):
    """View to update a gloss model from the jeditable jquery form
    We are sent one field and value at a time, return the new value once we've updated it."""

    # Get the gloss object or raise a Http404 exception if the object does not exist.
    gloss = get_object_or_404(Gloss, id=glossid)

    # Make sure that the user has rights to edit this datasets glosses.
    if 'view_dataset' not in get_perms(request.user, gloss.dataset):
        return HttpResponseForbidden(_("You do not have permissions to edit Glosses of this dataset/lexicon."))

    if request.method == "POST":
        # Update the user on Gloss.updated_by from request.user
        gloss.updated_by = request.user
        old_idgloss = str(gloss)

        field = request.POST.get('id', '')
        value = request.POST.get('value', '')

        if len(value) == 0:
            value = ' '

        elif value[0] == '_':
            value = value[1:]

        # in case we need multiple values
        values = request.POST.getlist('value[]')

        if field.startswith('keywords_'):
            if len(field.split('_')) == 2:
                # Gloss major
                language_code_2char = field.split('_')[1]
            elif len(field.split('_')) == 3:
                # Gloss secondary
                language_code_2char = field.split('_')[2]
            elif len(field.split('_')) == 4:
                # Gloss minor
                language_code_2char = field.split('_')[3]

            return update_keywords(gloss, field, value, language_code_2char=language_code_2char)

        elif field.startswith('relationforeign'):
            return update_relationtoforeignsign(gloss, field, value)
        # Had to add field != 'relation_between_articulators' because I changed its field name, and it conflicted here.
        elif field.startswith('relation') and field != 'relation_between_articulators':
            return update_relation(gloss, field, value)

        elif field.startswith('morphology-definition'):
            return update_morphology_definition(gloss, field, value)
        elif field == 'assigned_user':
            gloss.assigned_user_id = value if value and value.strip() != '' else None
            gloss.save()
            newvalue = gloss.assigned_user.get_full_name() if gloss.assigned_user else "None"
        elif field == 'dialect':
            # expecting possibly multiple values
            try:
                gloss.dialect.clear()
                for value in values:
                    lang = Dialect.objects.get(name=value)
                    gloss.dialect.add(lang)
                gloss.save()
                newvalue = ", ".join([str(g.name)
                                      for g in gloss.dialect.all()])
            except:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest("%s %s" % _("Unknown Dialect"), values, content_type='text/plain')
        elif field == 'wordclass':
            try:
                # Find fieldchoices that meet the wordclass association's limit choices
                # that match the provided machine values
                wordclasses = FieldChoice.objects.complex_filter(Gloss._meta.get_field(
                    "wordclasses").get_limit_choices_to()).filter(machine_value__in=values)
                gloss.wordclasses.set(wordclasses)
                gloss.save()
                newvalue = ", ".join([str(wc.english_name)
                                      for wc in gloss.wordclasses.all()])
            except:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest("%s %s" % _("Unknown wordclass"), values, content_type='text/plain')
        elif field == 'usage':
            try:
                # Find fieldchoices that meet the usage association's limit choices
                # that match the provided machine values
                usages = FieldChoice.objects.complex_filter(Gloss._meta.get_field(
                    "usage").get_limit_choices_to()).filter(machine_value__in=values)
                gloss.usage.set(usages)
                gloss.save()
                newvalue = "&#10;<br>".join([str(usage.english_name)
                                      for usage in gloss.usage.all()])
            except:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest("%s %s" % _("Unknown usage"), values, content_type='text/plain')
        elif field == 'semantic_field':
            try:
                # Find fieldchoices that meet the semantic_field association's limit choices
                # that match the provided machine values
                semantic_fields = FieldChoice.objects.complex_filter(Gloss._meta.get_field(
                    "semantic_field").get_limit_choices_to()).filter(machine_value__in=values)
                gloss.semantic_field.set(semantic_fields)
                gloss.save()
                newvalue = "&#10;<br>".join(str(semantic_field.english_name)
                                      for semantic_field in gloss.semantic_field.all())
            except:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest("%s '%s'" % (_("Unknown semantic_field"), str(values)), content_type='text/plain')
        elif field == 'lemma':
            value=str(value)
            value=value.strip()
            if value in ('',None):
                # Remove gloss's lemma
                try:
                    gloss.lemma = None
                    gloss.save()
                    newvalue = ''
                except:
                    return HttpResponseBadRequest("%s '%s'" % (_("Unknown Exception removing lemma"), str(value)), content_type='text/plain')
            else:
                # Change gloss's lemma
                try:
                    lemma = Lemma.objects.get(name=str(value))
                    gloss.lemma = lemma
                    gloss.save()
                    newvalue = str(lemma.name)
                except:
                    return HttpResponseBadRequest("%s %s" % (_("Unknown lemma"), str(value)), content_type='text/plain')
        elif field.startswith('video_title'):
            # If editing video title, update the GlossVideo's title
            if request.user.has_perm('video.change_glossvideo'):
                # Get pk after string "video_title"
                video_pk = field.split('video_title')[1]
                newvalue = value
                try:
                    video = GlossVideo.objects.get(pk=video_pk)
                    video.title = value
                    video.save()
                except GlossVideo.DoesNotExist:
                    return HttpResponseBadRequest('{error} {values}'.format(error=_('GlossVideo does not exist'), values=values),
                                                  content_type='text/plain')
            else:
                return HttpResponseForbidden('Missing permission: video.change_glossvideo')

        elif field.startswith('glossurl-'):
            if field == 'glossurl-create':
                GlossURL.objects.create(url=value, gloss_id=glossid)
                return HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': gloss.id}))
            else:
                if request.user.has_perm('dictionary.change_gloss'):
                    glossurl_pk = field.split('glossurl-')[1]
                    newvalue = value
                    try:
                        glossurl = GlossURL.objects.get(pk=glossurl_pk)
                        glossurl.url = value
                        glossurl.save()
                    except GlossURL.DoesNotExist:
                        pass

        else:
            # Find if field is not in Gloss classes fields.
            if field not in [f.name for f in Gloss._meta.get_fields()]:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest(_("Unknown field"), content_type='text/plain')

            # Translate the value if a boolean
            if isinstance(Gloss._meta.get_field(field), BooleanField):
                newvalue = value
                value = (value == 'Yes')

            # See if the field is a ForeignKey
            if gloss._meta.get_field(field).get_internal_type() == "ForeignKey":
                gloss.__setattr__(
                    field, FieldChoice.objects.get(
                        machine_value=value) if value and value.strip() != '' else None)
            else:
                gloss.__setattr__(field, value)
            gloss.save()

            # If the value is not a Boolean, return the new value
            if not isinstance(value, bool):
                f = Gloss._meta.get_field(field)
                # for choice fields we want to return the 'display' version of the value
                # Try to use get_choices to get correct choice names for FieldChoices
                # If it doesn't work, go to exception and get flatchoices
                try:
                    # valdict = dict(f.get_choices(include_blank=False))
                    valdict = dict(build_choice_list(field))
                except:
                    valdict = dict(f.flatchoices)

                # Some fields take ints
                # if valdict.keys() != [] and type(valdict.keys()[0]) == int:
                try:
                    newvalue = valdict.get(
                        int(value)) or valdict.get(value) or value
                except ValueError:  # Not an int
                    newvalue = valdict.get(value) or value

            # If field is idgloss and if the value has changed
            # Then change the filename on system and in glossvideo.videofile
            if field == 'idgloss' and newvalue != old_idgloss:
                try:
                    GlossVideo.rename_glosses_videos(gloss)
                except (OSError, IOError):
                    # Catch error, but don't do anything for now.
                    return HttpResponseServerError(_("Error: Unable to change videofiles names."))

        return HttpResponse(newvalue, content_type='text/plain')

    else:
        return HttpResponseNotAllowed(['POST'])


def update_keywords(gloss, field, value, language_code_2char):
    """Update the keyword field for the selected language"""

    # Try to get the language object based on the language_code.
    try:
        language = Language.objects.get(language_code_2char=language_code_2char)
    except Language.DoesNotExist:
        # If the language_code does not exist in any Language.language_code_2char, return 400 Bad Request.
        return HttpResponseBadRequest(_('A Language does not exist with language_code: ') + language_code_2char,
                                      content_type='text/plain')
    except Language.MultipleObjectsReturned:
        # If multiple Languages exist with the same language_code_2char
        return HttpResponseBadRequest(_('Multiple Languages with the same language_code exist, cannot edit because it '
                                        'is unclear which languages translations to edit.'),
                                      content_type='text/plain')

    (glosstranslations, created) = GlossTranslations.objects.get_or_create(gloss=gloss, language=language)

    if len(field.split('_')) == 2:
        glosstranslations.translations = value
    elif len(field.split('_')) == 3:
        glosstranslations.translations_secondary = value
    elif len(field.split('_')) == 4:
        glosstranslations.translations_minor = value

    glosstranslations.save()
    # Save updated_by and updated_at field for Gloss
    gloss.save()

    return HttpResponse(value, content_type='text/plain')


def update_relation(gloss, field, value):
    """Update one of the relations for this gloss"""

    (what, relid) = field.split('_')
    what = what.replace('-', '_')

    try:
        rel = Relation.objects.get(id=relid)
    except Relation.DoesNotExist:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest("%s '%s'" % _("Bad Relation ID"), relid, content_type='text/plain')

    if not rel.source == gloss:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest(_("Relation doesn't match gloss"), content_type='text/plain')

    if what == 'relationdelete':
        print(("DELETE: ", rel))
        rel.delete()
        return HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': gloss.id}))
    elif what == 'relationrole':
        # rel.role = value
        try:
            rel.role = FieldChoice.objects.get(machine_value=value)
        except FieldChoice.DoesNotExist:
            rel.role = value
        rel.save()
        # newvalue = rel.get_role_display()
        newvalue = rel.role
    elif what == 'relationtarget':

        target = gloss_from_identifier(value)
        if target:
            rel.target = target
            rel.save()
            newvalue = str(target)
        else:
            # Translators: HttpResponseBadRequest
            return HttpResponseBadRequest("%s '%s'" % _("Badly formed gloss identifier"), value,
                                          content_type='text/plain')
    else:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest("%s '%s'" % _("Unknown form field"), field, content_type='text/plain')

    return HttpResponse(newvalue, content_type='text/plain')


def update_relationtoforeignsign(gloss, field, value):
    """Update one of the relations for this gloss"""

    (what, relid) = field.split('_')
    what = what.replace('-', '_')

    try:
        rel = RelationToForeignSign.objects.get(id=relid)
    except RelationToForeignSign.DoesNotExist:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest("%s '%s'" % _("Bad RelationToForeignSign ID"), relid,
                                      content_type='text/plain')

    if not rel.gloss == gloss:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest(_("Relation doesn't match gloss"), content_type='text/plain')

    if what == 'relationforeigndelete':
        print(("DELETE: ", rel))
        rel.delete()
        return HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': gloss.id}))
    elif what == 'relationforeign_loan':
        rel.loan = value == 'Yes'
        rel.save()

    elif what == 'relationforeign_other_lang':
        rel.other_lang = value
        rel.save()

    elif what == 'relationforeign_other_lang_gloss':
        rel.other_lang_gloss = value
        rel.save()

    else:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest("%s '%s'" % _("Unknown form field"), field, content_type='text/plain')

    return HttpResponse(value, content_type='text/plain')


def gloss_from_identifier(value):
    """Given an id of the form idgloss (pk) return the
    relevant gloss or None if none is found"""

    # We need another way to add a Relation to a Gloss. One textfield can't serve all the possible ways of adding.
    # One possible solution is to add two fields, one that serves adding by ID and other with Gloss name or name+id.
    # However, no one is going to memorize or check for the id numbers and they will probably add with Gloss name only.
    # Therefore the only useful implementation is to do it with the Gloss name only or with Glossname + id.
    # TODO: Decide what to do here

    """
    # See if 'value' is an int, should match if the user uses only an 'id' as a search string
    try:
        int(value)
        is_int = True
    except:
        is_int = False
    # If value is already int, try to match the int as IDGloss id.
    if is_int:
        try:
            target = Gloss.objects.get(pk=int(value))
        except ObjectDoesNotExist:
            # If the int doesn't match anything, return
            return HttpResponseBadRequest(_("Target gloss not found."), content_type='text/plain')

        return target
    # If 'value' is not int, then try to catch a string like "CAMEL (10)"
    else:"""

    # This regex looks from the Beginning of a string for IDGLOSS and then the id
    # For example: "CAMEL (10)", idgloss="CAMEL" and pk=10
    match = re.match('(.*) \((\d+)\)', value)

    if match:
        # print "MATCH: ", match
        idgloss = match.group(1)
        pk = match.group(2)
        # print "INFO: ", idgloss, pk
        # Try if target Gloss exists, if not, assign None to target, then it returns None
        try:
            target = Gloss.objects.get(pk=int(pk))
        except ObjectDoesNotExist:
            target = None
        # print "TARGET: ", target
        return target
    # If regex doesn't match, return None
    else:
        return None


def add_relation(request):
    """Add a new relation instance"""

    if request.method == "POST":

        form = RelationForm(request.POST)

        if form.is_valid():

            role = form.cleaned_data['role']
            sourceid = form.cleaned_data['sourceid']
            targetid = form.cleaned_data['targetid']

            try:
                source = Gloss.objects.get(pk=int(sourceid))
            except Gloss.DoesNotExist:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest(_("Source gloss not found."), content_type='text/plain')

            target = gloss_from_identifier(targetid)

            if target:
                rel = Relation(source=source, target=target, role=role)
                rel.save()

                return HttpResponseRedirect(
                    reverse('dictionary:admin_gloss_view', kwargs={'pk': source.id}) + '?editrel')
            else:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest(_("Target gloss not found."), content_type='text/plain')
        else:
            print(form)

    # fallback to redirecting to the requesting page
    return HttpResponseRedirect('/')


def add_relationtoforeignsign(request):
    """Add a new relationtoforeignsign instance"""

    if request.method == "POST":

        form = RelationToForeignSignForm(request.POST)

        if form.is_valid():

            sourceid = form.cleaned_data['sourceid']
            loan = form.cleaned_data['loan']
            other_lang = form.cleaned_data['other_lang']
            other_lang_gloss = form.cleaned_data['other_lang_gloss']

            try:
                gloss = Gloss.objects.get(pk=int(sourceid))
            except Gloss.DoesNotExist:
                # Translators: HttpResponseBadRequest
                return HttpResponseBadRequest(_("Source gloss not found."), content_type='text/plain')

            rel = RelationToForeignSign(gloss=gloss, loan=loan, other_lang=other_lang,
                                        other_lang_gloss=other_lang_gloss)
            rel.save()

            return HttpResponseRedirect(
                reverse('dictionary:admin_gloss_view', kwargs={'pk': gloss.id}) + '?editrelforeign')

        else:
            print(form)
            # Translators: HttpResponseBadRequest
            return HttpResponseBadRequest(_("Form not valid"), content_type='text/plain')

    # fallback to redirecting to the requesting page
    return HttpResponseRedirect('/')


def add_morphology_definition(request):
    if request.method == "POST":
        form = MorphologyForm(request.POST)

        if form.is_valid():
            parent_gloss = form.cleaned_data['parent_gloss_id']
            role = form.cleaned_data['role']
            morpheme_id = form.cleaned_data['morpheme_id']
            morpheme = gloss_from_identifier(morpheme_id)

            thisgloss = get_object_or_404(Gloss, pk=parent_gloss)

            # create definition, default to not published
            morphdef = MorphologyDefinition(
                parent_gloss=thisgloss, role=role, morpheme=morpheme)
            morphdef.save()

            return HttpResponseRedirect(
                reverse('dictionary:admin_gloss_view', kwargs={'pk': thisgloss.id}) + '?editmorphdef')
    # Translators: Htt404
    raise Http404(_('Incorrect request'))


def update_morphology_definition(gloss, field, value):
    """Update one of the relations for this gloss"""

    (what, morph_def_id) = field.split('_')
    what = what.replace('-', '_')

    try:
        morph_def = MorphologyDefinition.objects.get(id=morph_def_id)
    except MorphologyDefinition.DoesNotExist:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest("%s '%s'" % _("Bad Morphology Definition ID"), morph_def_id,
                                      content_type='text/plain')

    if not morph_def.parent_gloss == gloss:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest(_("Morphology Definition doesn't match gloss"), content_type='text/plain')

    if what == 'morphology_definition_delete':
        print(("DELETE: ", morph_def))
        morph_def.delete()
        return HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': gloss.id}))
    elif what == 'morphology_definition_role':
        # morph_def.role = value
        morph_def.role = FieldChoice.objects.get(machine_value=value)
        morph_def.save()
        # newvalue = morph_def.get_role_display()
        newvalue = morph_def.role.english_name
    elif what == 'morphology_definition_morpheme':

        morpheme = gloss_from_identifier(value)
        if morpheme:
            morph_def.morpheme = morpheme
            morph_def.save()
            newvalue = str(morpheme)
        else:
            # Translators: HttpResponseBadRequest
            return HttpResponseBadRequest("%s '%s'" % _("Badly formed gloss identifier"), value,
                                          content_type='text/plain')
    else:
        # Translators: HttpResponseBadRequest
        return HttpResponseBadRequest("%s '%s'" % _("Unknown form field"), field, content_type='text/plain')

    return HttpResponse(newvalue, content_type='text/plain')


def add_lemma(request, glossid):
    value = str(request.POST.get('value', ''))
    value=value.strip()

    # Default response is return to page
    default_response = HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': glossid}))
    response = default_response

    if (not value) or value in (None, ''):
        # Tried to add a blank/empty lemma
        return default_response

    if request.method == "POST":
        lemma = Lemma.objects.filter(name=value)
        if lemma:
            # Lemma already exists
            return default_response

        # Add the new lemma to the system
        try:
            lemma=Lemma(name=value)
            lemma.save()
            response = HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': glossid}))
        except:
            response = HttpResponseBadRequest("%s '%s'" % (_("Invalid Lemma name"), value),
                                                content_type='text/plain')
    return response


@permission_required('dictionary.change_gloss')
def add_tag(request, glossid):
    """View to add a tag to a gloss"""

    # default response
    response = HttpResponse('invalid', content_type='text/plain')

    if request.method == "POST":
        gloss = get_object_or_404(Gloss, id=glossid)
        if 'view_dataset' not in get_perms(request.user, gloss.dataset):
            # If user has no permissions to dataset, raise PermissionDenied to show 403 template.
            msg = _("You do not have permissions to add tags to glosses of this lexicon.")
            messages.error(request, msg)
            raise PermissionDenied(msg)

        form = TagDeleteForm(request.POST)
        if form.is_valid():
            if form.cleaned_data['delete']:
                tag = form.cleaned_data['tag']
                # get the relevant TaggedItem
                ti = get_object_or_404(
                    TaggedItem, object_id=gloss.id, tag__name=tag,
                    content_type=ContentType.objects.get_for_model(Gloss))
                ti.delete()
                response = HttpResponse(
                    'deleted', content_type='text/plain')
                return response

        form = TagUpdateForm(request.POST)
        if form.is_valid():
            tag = form.cleaned_data['tag']

            # we need to wrap the tag name in quotes since it might contain spaces
            Tag.objects.add_tag(gloss, '"%s"' % tag)
            # response is new HTML for the tag list and form
            response = render(request, 'dictionary/glosstags.html',
                              {'gloss': gloss, 'tagsaddform': TagsAddForm()})

        else:
            # If we are adding (multiple) tags, this form should validate.
            form = TagsAddForm(request.POST)
            if form.is_valid():
                tags = form.cleaned_data['tags']
                for tag in tags:
                    add_tags_to_gloss(gloss, tag)

                response = render(request, 'dictionary/glosstags.html',
                                  {'gloss': gloss, 'tagsaddform': TagsAddForm()})

    return response


# We are using this custom-made function instead of the in-built due to the incorrect handling of tags which contains
# spaces.
def add_tags_to_gloss(gloss, tag):
    tag = Tag.objects.filter(name=tag.name).first()
    c_type = ContentType.objects.get_for_model(gloss)
    TaggedItem._default_manager.get_or_create(
        tag=tag, content_type=c_type, object_id=gloss.pk)


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
    "description",
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
@permission_required('dictionary.import_csv')
def import_nzsl_share_gloss_csv(request):
    """
    Check which objects exist and which not. Then show the user a list of glosses that will be added if user confirms.
    Store the glosses to be added into sessions.
    """
    # Make sure that the session variables are flushed before using this view.
    if 'dataset_id' in request.session: del request.session['dataset_id']
    if 'glosses_new' in request.session: del request.session['glosses_new']

    new_glosses = []
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
                glossreader = csv.DictReader(
                    codecs.iterdecode(form.cleaned_data['file'], 'utf-8'),
                    fieldnames=share_csv_header_list,
                    delimiter=',',
                    quotechar='"'
                )
                for row in glossreader:
                    if glossreader.line_num == 1:
                        continue
                    new_glosses.append(row)
            except csv.Error as e:
                # Can't open file, remove session variables
                if 'dataset_id' in request.session: del request.session['dataset_id']
                if 'glosses_new' in request.session: del request.session['glosses_new']
                # Set a message to be shown so that the user knows what is going on.
                messages.add_message(request, messages.ERROR, _('Cannot open the file:' + str(e)))
                return render(request, 'dictionary/import_nzsl_share_gloss_csv.html', {'import_csv_form': CSVUploadForm()}, )
            except UnicodeDecodeError as e:
                # File is not UTF-8 encoded.
                messages.add_message(request, messages.ERROR, _('File must be UTF-8 encoded!'))
                return render(request, 'dictionary/import_nzsl_share_gloss_csv.html', {'import_csv_form': CSVUploadForm()}, )

            # Store dataset's id and the list of glosses to be added in session.
            request.session['dataset_id'] = dataset.id
            request.session['glosses_new'] = new_glosses

            return render(request, 'dictionary/import_nzsl_share_gloss_csv_confirmation.html',
                          {'glosses_new': new_glosses,
                           'dataset': dataset, })
        else:
            # If form is not valid, set a error message and return to the original form.
            messages.add_message(request, messages.ERROR, _('The provided CSV-file does not meet the requirements '
                                                            'or there is some other problem.'))
            return render(request, 'dictionary/import_nzsl_share_gloss_csv.html', {'import_csv_form': form}, )
    else:
        # If request type is not POST, return to the original form.
        csv_form = CSVUploadForm()
        allowed_datasets = get_objects_for_user(request.user, 'dictionary.view_dataset')
        # Make sure we only list datasets the user has permissions to.
        csv_form.fields["dataset"].queryset = csv_form.fields["dataset"].queryset.filter(
            id__in=[x.id for x in allowed_datasets])
        return render(request, "dictionary/import_nzsl_share_gloss_csv.html",
                      {'import_csv_form': csv_form}, )


@login_required
@permission_required('dictionary.import_csv')
@transaction.atomic()
def confirm_import_nzsl_share_gloss_csv(request):
    """This view adds the data to database if the user confirms the action"""
    if request.method == 'POST':
        if 'cancel' in request.POST:
            # If user cancels adding data, flush session variables
            if 'dataset_id' in request.session: del request.session['dataset_id']
            if 'glosses_new' in request.session: del request.session['glosses_new']
            # Set a message to be shown so that the user knows what is going on.
            messages.add_message(request, messages.WARNING, _('Cancelled adding CSV data.'))
            return HttpResponseRedirect(reverse('dictionary:import_nzsl_share_gloss_csv'))

        elif 'confirm' in request.POST:
            glosses_added = []
            dataset = None
            translations = []
            comments = []
            videos = []
            new_glosses = {}
            bulk_create_gloss = []
            bulk_update_glosses = []
            bulk_semantic_fields = []

            if 'glosses_new' and 'dataset_id' in request.session:
                dataset = Dataset.objects.get(id=request.session['dataset_id'])
                language_en = Language.objects.get(name="English")
                language_mi = Language.objects.get(name="Māori")
                gloss_content_type = ContentType.objects.get_for_model(Gloss)
                video_type = FieldChoice.objects.get(field="video_type", english_name="validation")
                site = Site.objects.get_current()
                comment_submit_date = datetime.datetime.now(tz=get_current_timezone())
                semantic_fields = FieldChoice.objects.filter(
                    field="semantic_field"
                ).values_list("english_name", "pk")
                semantic_fields_dict = {field[0]: field[1] for field in semantic_fields}

                for row_num, gloss in enumerate(request.session['glosses_new']):
                    # will iterate over these glosses again after bulk creating and to make sure
                    # we preserve order
                    new_glosses[str(row_num)] = gloss
                    bulk_create_gloss.append(Gloss(
                        dataset=dataset,
                        # need to make idgloss unique in dataset,
                        # but gloss word can appear in multiple rows, so
                        # idgloss will be updated to word:pk in second step
                        idgloss=f"{gloss['word']}_row{row_num}",
                        idgloss_mi=gloss.get("maori", None),
                        notes=gloss.get("notes", ""),
                        created_by=request.user,
                        updated_by=request.user
                    ))

                bulk_created = Gloss.objects.bulk_create(bulk_create_gloss)
                idglosses = [x.idgloss for x in bulk_created]
                bulk_created_with_pk = Gloss.objects.filter(
                    idgloss__in=idglosses
                )

                gloss_pks = []

                for gloss in bulk_created_with_pk:
                    gloss_pks.append(gloss.pk)
                    old_id_gloss = gloss.idgloss.split("_row")
                    gloss_data = new_glosses[old_id_gloss[1]]

                    if gloss_data.get("topic_names", None):
                        gloss_topics = gloss_data["topic_names"].split("|")
                        cleaned_gloss_topics = [x for x in gloss_topics if x not in ["all signs", "All signs"]]
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
                                add_miscellaneous = True
                        if add_miscellaneous:
                            bulk_semantic_fields.append(
                                Gloss.semantic_field.through(
                                    gloss_id=gloss.id,
                                    fieldchoice_id=semantic_fields_dict["Miscellaneous"]
                                )
                            )

                    # Prepare new idgloss fields for bulk update
                    gloss.idgloss = f"{old_id_gloss[0]}:{gloss.pk}"
                    if gloss.idgloss_mi:
                        gloss.idgloss_mi = f"{gloss.idgloss_mi}:{gloss.pk}"
                    bulk_update_glosses.append(gloss)

                    translations.append(GlossTranslations(
                        gloss=gloss,
                        language=language_en,
                        translations=gloss_data["word"],
                        translations_secondary=gloss_data.get("secondary", None)
                    ))
                    if gloss_data.get("maori", None):
                        translations.append(GlossTranslations(
                            gloss=gloss,
                            language=language_mi,
                            translations=gloss_data["maori"]
                        ))

                    comments.append(Comment(
                        content_type=gloss_content_type,
                        object_pk=gloss.pk,
                        user_name=gloss_data.get("contributor_name", ""),
                        user_email=gloss_data.get("contributor_email", ""),
                        comment=gloss_data.get("notes", ""),
                        site=site,
                        is_public=False,
                        submit_date=comment_submit_date
                    ))
                    if gloss_data.get("sign_comments", None):
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

                    if gloss_data.get("videos", None):
                        for i, video_url in enumerate(gloss_data["videos"].split("|")):
                            extension = video_url[-3:]
                            file_name = (
                                f"{settings.MEDIA_ROOT}/glossvideo/"
                                f"{gloss.pk}-{gloss.idgloss}_video_{i+1}.{extension}"
                            )
                            if i == 0:
                                title = "Main"
                            else:
                                title = f"Video_{i+1}"

                            glossvideo = {
                                "url": video_url,
                                "file_name": file_name,
                                "gloss_pk": gloss.pk,
                                "title": title,
                                "version": i
                            }
                            videos.append(glossvideo)

                    if gloss_data.get("illustrations", None):
                        for i, video_url in enumerate(gloss_data["illustrations"].split("|")):
                            extension = video_url[-3:]
                            file_name = (
                                f"{settings.MEDIA_ROOT}/glossvideo/"
                                f"{gloss.pk}-{gloss.idgloss}_illustration_{i+1}.{extension}"
                            )

                            glossvideo = {
                                "url": video_url,
                                "file_name": file_name,
                                "gloss_pk": gloss.pk,
                                "title": f"Illustration_{i+1}",
                                "version": i
                            }
                            videos.append(glossvideo)

                    if gloss_data.get("usage_examples", None):
                        for i, video_url in enumerate(gloss_data["usage_examples"].split("|")):
                            extension = video_url[-3:]
                            file_name = (
                                f"{settings.MEDIA_ROOT}/glossvideo/"
                                f"{gloss.pk}-{gloss.idgloss}_usageexample_{i+1}.{extension}"
                            )
                            if i <= 1:
                                title = f"finalexample{i+1}",
                            else:
                                title = f"UsageExample_{i+1}",

                            glossvideo = {
                                "url": video_url,
                                "file_name": file_name,
                                "gloss_pk": gloss.pk,
                                "title": title,
                                "version": i
                            }
                            videos.append(glossvideo)

                    glosses_added.append(gloss)

                # Bulk create entities related to the gloss, and bulk update the glosses' idgloss
                Comment.objects.bulk_create(comments)
                GlossTranslations.objects.bulk_create(translations)
                Gloss.objects.bulk_update(bulk_update_glosses, ["idgloss", "idgloss_mi"])
                Gloss.semantic_field.through.objects.bulk_create(bulk_semantic_fields)

                # call background task to retrieve videos for glosses and activate the task
                # processor for 10 minutes
                retrieve_videos_for_glosses(gloss_pks, videos)
                call_command("process_tasks", "--duration=600 --sleep=60 --log-std")

                # Flush request.session['glosses_new'] and request.session['dataset']
                del request.session['glosses_new']
                del request.session['dataset_id']

                # Set a message to be shown so that the user knows what is going on.
                # messages.add_message(request, messages.SUCCESS, _('Glosses were added succesfully.'))
            return render(request, "dictionary/import_nzsl_share_gloss_csv_confirmation.html", {'glosses_added': glosses_added,
                                                                                     'dataset': dataset.name})
        else:
            return HttpResponseRedirect(reverse('dictionary:import_nzsl_share_gloss_csv'))
    else:
        # If request method is not POST, redirect to the import form
        return HttpResponseRedirect(reverse('dictionary:import_nzsl_share_gloss_csv'))


def gloss_relation(request):
    """Processes Gloss Relations"""
    if request.method == "POST":
        form = GlossRelationForm(request.POST)
        if "delete" in form.data:
            glossrelation = get_object_or_404(GlossRelation, id=int(form.data["delete"]))
            if 'view_dataset' not in get_perms(request.user, glossrelation.source.dataset):
                # If user has no permissions to dataset, raise PermissionDenied to show 403 template.
                msg = _("You do not have permissions to delete relations from glosses of this lexicon.")
                messages.error(request, msg)
                raise PermissionDenied(msg)
            ct = ContentType.objects.get_for_model(GlossRelation)
            # Delete TaggedItems and the GlossRelation
            TaggedItem.objects.filter(object_id=glossrelation.id, content_type=ct).delete()
            glossrelation.delete()

            if "HTTP_REFERER" in request.META:
                return redirect(request.META["HTTP_REFERER"])
            return redirect("/")

        if form.is_valid():
            source = get_object_or_404(Gloss, id=form.cleaned_data["source"])
            if 'view_dataset' not in get_perms(request.user, source.dataset):
                # If user has no permissions to dataset, raise PermissionDenied to show 403 template.
                msg = _("You do not have permissions to add relations to glosses of this lexicon.")
                messages.error(request, msg)
                raise PermissionDenied(msg)
            target = get_object_or_404(Gloss, id=form.cleaned_data["target"])
            glossrelation = GlossRelation.objects.create(source=source, target=target)
            if form.cleaned_data["tag"]:
                TaggedItem.objects.create(
                    object=glossrelation, tag=form.cleaned_data["tag"])
            if "HTTP_REFERER" in request.META:
                return redirect(request.META["HTTP_REFERER"])
            return redirect("/")

        return HttpResponseBadRequest("Bad request.")

    return HttpResponseForbidden()
