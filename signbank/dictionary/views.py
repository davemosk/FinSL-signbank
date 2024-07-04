# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os

import json
import time

from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.shortcuts import render
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import permission_required
from django.contrib.admin.views.decorators import user_passes_test
from django.core.exceptions import PermissionDenied
from django.urls import reverse, reverse_lazy
from django.utils.translation import gettext as _
from django.views.generic.list import ListView
from django.views.generic import FormView
from django.db.models import Q, F, Count, Case, Value, When, BooleanField
from urllib.parse import quote
from wsgiref.util import FileWrapper

from tagging.models import Tag
from guardian.shortcuts import get_perms, get_objects_for_user, get_users_with_perms
from notifications.signals import notify

from signbank.dictionary.models import Dataset, Keyword, FieldChoice, Gloss, GlossRelation
from signbank.dictionary.forms import GlossCreateForm, LexiconForm
from signbank.dictionary import tools
from signbank.dictionary.update import add_tags_to_gloss

from signbank.video.models import GlossVideo
from signbank.video.forms import GlossVideoForm


@permission_required('dictionary.add_gloss')
def create_gloss(request):
    """Handle Gloss creation."""
    if request.method == 'POST':
        form = GlossCreateForm(request.POST)
        glossvideoform = GlossVideoForm(request.POST, request.FILES)
        glossvideoform.fields['videofile'].required=False
        if form.is_valid() and glossvideoform.is_valid():
            if 'view_dataset' not in get_perms(request.user, form.cleaned_data["dataset"]):
                # If user has no permissions to dataset, raise PermissionDenied to show 403 template.
                msg = _("You do not have permissions to create glosses for this lexicon.")
                messages.error(request, msg)
                raise PermissionDenied(msg)

            new_gloss = form.save(commit=False)
            new_gloss.created_by = request.user
            new_gloss.updated_by = request.user
            new_gloss.save()
            if form.cleaned_data["tag"]:
                tag = Tag.objects.filter(name=form.cleaned_data["tag"].name).first()
                add_tags_to_gloss(new_gloss, tag)
            if glossvideoform.cleaned_data['videofile']:
                glossvideo = glossvideoform.save(commit=False)
                glossvideo.gloss = new_gloss
                glossvideo.save()
            return HttpResponseRedirect(reverse('dictionary:admin_gloss_view', kwargs={'pk': new_gloss.pk}))

        else:
            # Return bound fields with errors if the form is not valid.
            allowed_datasets = get_objects_for_user(request.user, 'dictionary.view_dataset')
            form.fields["dataset"].queryset = Dataset.objects.filter(id__in=[x.id for x in allowed_datasets])
            return render(request, 'dictionary/create_gloss.html', {'form': form, 'glossvideoform': glossvideoform})
    else:
        allowed_datasets = get_objects_for_user(request.user, 'dictionary.view_dataset')
        form = GlossCreateForm()
        glossvideoform = GlossVideoForm()
        form.fields["dataset"].queryset = Dataset.objects.filter(id__in=[x.id for x in allowed_datasets])
        return render(request, 'dictionary/create_gloss.html', {'form': form, 'glossvideoform': glossvideoform})


def keyword_value_list(request, prefix=None):
    """View to generate a list of possible values for a keyword given a prefix."""
    kwds = Keyword.objects.filter(text__startswith=prefix)
    kwds_list = [k.text for k in kwds]
    return HttpResponse("\n".join(kwds_list), content_type='text/plain')


@user_passes_test(lambda u: u.is_staff, login_url='/accounts/login/')
def try_code(request):
    """A view for the developer to try out things"""
    choicedict = {}
    for key, choices in list(choicedict.items()):
        for machine_value, english_name in choices:
            FieldChoice(
                english_name=english_name, field=key, machine_value=machine_value).save()
    return HttpResponse('OK', status=200)


class ManageLexiconsListView(ListView):
    model = Dataset
    template_name = 'dictionary/manage_lexicons.html'
    paginate_by = 50

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        qs = self.get_queryset()
        context['has_permissions'] = qs.filter(has_view_perm=True)
        context['no_permissions'] = qs.filter(has_view_perm=False)
        # Show users with permissions to lexicons to SuperUsers
        if self.request.user.is_superuser:
            for lexicon in context['has_permissions']:
                lexicon.users_with_perms = get_users_with_perms(obj=lexicon, with_superusers=True)
            for lexicon in context['no_permissions']:
                lexicon.users_with_perms = get_users_with_perms(obj=lexicon, with_superusers=True)
        return context

    def get_queryset(self):
        # Get allowed datasets for user (django-guardian)
        allowed_datasets = get_objects_for_user(self.request.user, 'dictionary.view_dataset')
        # Get queryset
        qs = super().get_queryset()
        qs = qs.annotate(
            has_view_perm=Case(
                When(Q(id__in=allowed_datasets), then=Value(True)),
                default=Value(False), output_field=BooleanField()))
        qs = qs.select_related('signlanguage')
        return qs


class ApplyLexiconPermissionsFormView(FormView):
    form_class = LexiconForm
    template_name = 'dictionary/manage_lexicons.html'
    success_url = reverse_lazy('dictionary:manage_lexicons')

    def form_valid(self, form):
        dataset = form.cleaned_data['dataset']
        admins = dataset.admins.all()
        notify.send(sender=self.request.user, recipient=admins,
                    verb="{txt} {dataset}".format(txt=_("applied for permissions to:"), dataset=dataset.public_name),
                    action_object=self.request.user,
                    description="{user} ({user.first_name} {user.last_name}) {txt} {dataset}".format(
                        user=self.request.user, txt=_("applied for permissions to lexicon:"),
                        dataset=dataset.public_name
                    ),
                    target=self.request.user, public=False)
        msg = "{text} {lexicon_name}".format(text=_("Successfully applied permissions for"), lexicon_name=dataset.public_name)
        messages.success(self.request, msg)
        return super().form_valid(form)


def network_graph(request):
    """Network graph of GlossRelations"""
    context = dict()
    form = LexiconForm(request.GET, use_required_attribute=False)
    # Get allowed datasets for user (django-guardian)
    allowed_datasets = get_objects_for_user(request.user, 'dictionary.view_dataset')
    # Filter the forms dataset field for the datasets user has permission to.
    form.fields["dataset"].queryset = Dataset.objects.filter(id__in=[x.id for x in allowed_datasets])
    dataset = None
    if form.is_valid():
        form.fields["dataset"].widget.is_required = False
        dataset = form.cleaned_data["dataset"]

    if dataset:
        context["dataset"] = dataset
        nodeqs = Gloss.objects.filter(Q(dataset=dataset),
                                      Q(glossrelation_target__isnull=False) | Q(glossrelation_source__isnull=False))\
            .distinct().values("id").annotate(label=F("idgloss"), size=Count("glossrelation_source")+Count("glossrelation_target"))
        context["nodes"] = json.dumps(list(nodeqs))
        edgeqs = GlossRelation.objects.filter(Q(source__dataset=dataset) | Q(target__dataset=dataset)).values("id", "source", "target")
        context["edges"] = json.dumps(list(edgeqs))
    return render(request, "dictionary/network_graph.html",
                  {'context': context,
                   'form': form
                   })


def package(request):
    """
    This view is copied from Global Signbank.
    It has been adapted to work for NZSL's data structure.
    """
    if request.user.is_authenticated:
        if 'dataset_name' in request.GET:
            dataset = Dataset.objects.get(name=request.GET['dataset_name'])
        else:
            dataset = Dataset.objects.get(name=settings.DEFAULT_DATASET_ACRONYM)
        available_glosses = Gloss.objects.filter(dataset=dataset)
    else:
        dataset = Dataset.objects.get(name=settings.DEFAULT_DATASET_ACRONYM)
        available_glosses = Gloss.objects.filter(dataset=dataset)

    first_part_of_file_name = 'signbank_pa'

    timestamp_part_of_file_name = str(int(time.time()))

    if 'since_timestamp' in request.GET:
        first_part_of_file_name += 'tch'
        since_timestamp = int(request.GET['since_timestamp'])
        timestamp_part_of_file_name = request.GET[
                                          'since_timestamp'] + '-' + timestamp_part_of_file_name
    else:
        first_part_of_file_name += 'ckage'
        since_timestamp = 0

    archive_file_name = '.'.join([first_part_of_file_name, timestamp_part_of_file_name, 'zip'])
    archive_file_path = settings.SIGNBANK_PACKAGES_FOLDER + "/" + archive_file_name

    available_glossvideos = GlossVideo.objects.filter(gloss__in=available_glosses)

    video_urls = {
        os.path.splitext(os.path.basename(gv.videofile.name))[0]: reverse(
            'dictionary:protected_media',
            kwargs={"filename": gv.videofile.name}
        )
        for gv in available_glossvideos
        if os.path.exists(str(gv.videofile.path))
        and os.path.getmtime(str(gv.videofile.path)) > since_timestamp
        and gv.is_video()
    }

    image_urls = {
        os.path.splitext(os.path.basename(gv.videofile.name))[0]: reverse(
            'dictionary:protected_media', kwargs={"filename": gv.videofile.name}
        )
        for gv in available_glossvideos
        if os.path.exists(str(gv.videofile.path))
        and os.path.getmtime(str(gv.videofile.path)) > since_timestamp
        and gv.is_image()
    }

    collected_data = {'video_urls': video_urls,
                      'image_urls': image_urls,
                      'glosses': tools.get_gloss_data(since_timestamp, dataset)}

    tools.create_zip_with_json_files(collected_data, archive_file_path)

    response = HttpResponse(FileWrapper(open(archive_file_path, 'rb')),
                            content_type='application/zip')
    response['Content-Disposition'] = 'attachment; filename=' + archive_file_name
    return response


def info(request):
    """
    This view is copied from Global Signbank.
    It has been adapted to work for NZSL's data structure.
    """
    user_datasets = get_objects_for_user(request.user, 'change_dataset',
                                                            Dataset)
    user_datasets_names = [dataset.name for dataset in user_datasets]

    # Put the default dataset in first position
    if settings.DEFAULT_DATASET_ACRONYM in user_datasets_names:
        user_datasets_names.insert(0, user_datasets_names.pop(
            user_datasets_names.index(settings.DEFAULT_DATASET_ACRONYM)))

    if user_datasets_names:
        return HttpResponse(json.dumps(user_datasets_names), content_type='application/json')
    else:
        return HttpResponse(json.dumps([settings.LANGUAGE_NAME, settings.COUNTRY_NAME]),
                            content_type='application/json')


def protected_media(request, filename, show_indexes=False):
    """
    This view is copied from Global Signbank.
    It has been adapted to work for NZSL's data structure.
    """
    if not request.user.is_authenticated:

        # If we are not logged in, try to find if this maybe belongs to a gloss that is free to see for everbody?
        (name, ext) = os.path.splitext(os.path.basename(filename))
        if 'handshape' in name:
            # handshape images are allowed to be seen in Show All Handshapes
            pass
        else:
            gloss_pk = int(filename.split('.')[-2].split('-')[-1])

            try:
                Gloss.objects.get(pk=gloss_pk)
            except Gloss.DoesNotExist:
                return HttpResponse(status=401)

        # If we got here, the gloss was found and in the web dictionary, so we can continue

    filename = os.path.normpath(filename)

    dir_path = settings.WRITABLE_FOLDER
    path = dir_path.encode('utf-8') + filename.encode('utf-8')

    if not os.path.exists(path):
        # quote the filename instead to resolve special characters in the url
        (head, tail) = os.path.split(filename)
        quoted_filename = quote(tail, safe='')
        quoted_path = os.path.join(dir_path, head, quoted_filename)
        if not os.path.exists(quoted_path):
            raise Http404("File does not exist.")
        else:
            filename = quoted_filename
            path = quoted_path

    if not settings.USE_X_SENDFILE:
        if filename.split('.')[-1] == 'mp4':
            response = HttpResponse(content_type='video/mp4')
        elif filename.split('.')[-1] == 'png':
            response = HttpResponse(content_type='image/png')
        elif filename.split('.')[-1] == 'jpg':
            response = HttpResponse(content_type='image/jpg')
        else:
            response = HttpResponse()

        response['Content-Disposition'] = 'inline;filename=' + filename + ';filename*=UTF-8'
        response['X-Sendfile'] = path

        return response

    else:
        from django.views.static import serve
        return serve(request, filename, dir_path, show_indexes)
