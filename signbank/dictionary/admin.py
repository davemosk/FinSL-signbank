# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as AuthUserAdmin
from django.contrib.auth.models import User
from django.contrib.contenttypes.admin import GenericTabularInline
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.forms import CheckboxSelectMultiple, ModelForm, Textarea
from django.utils.translation import gettext as _
from guardian.admin import GuardedModelAdmin
from modeltranslation.admin import TranslationAdmin as ModelTranslationAdmin
from reversion.admin import VersionAdmin
from tagging.models import Tag, TaggedItem

from .models import (AllowedTags, Dataset, Dialect, FieldChoice, Gloss, Lemma,
                     GlossRelation, GlossTranslations, GlossURL, Language,
                     ManualValidationAggregation, ShareValidationAggregation,
                     SignLanguage, Translation, ValidationRecord)
from ..video.admin import GlossVideoInline


class TagListFilter(admin.SimpleListFilter):
    title = _('Tag')
    parameter_name = 'tag'

    def lookups(self, request, model_admin):
        tags = Tag.objects.usage_for_model(model_admin.model)
        return [(tag.name, _(tag.name)) for tag in tags]

    def queryset(self, request, queryset):
        if self.value():
            ct = ContentType.objects.get_for_model(queryset.model)
            return queryset.filter(id__in=[x.object_id for x in TaggedItem.objects.filter(tag__name=self.value(),
                                                                                          content_type=ct)])


class DatasetAdmin(GuardedModelAdmin, ModelTranslationAdmin):
    model = Dataset
    list_display = ('name', 'is_public', 'signlanguage',)


class TranslationAdmin(admin.ModelAdmin):
    search_fields = ['^keyword__text', '^gloss__idgloss']
    list_filter = ('gloss__dataset',)
    list_display = ('gloss', 'keyword')


class TranslationInline(admin.TabularInline):
    model = Translation
    extra = 0

    def get_readonly_fields(self, request, obj=None):
        # Set all fields to be read only.
        return list(set(
            [field.name for field in self.opts.local_fields] +
            [field.name for field in self.opts.local_many_to_many]
        ))

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class AllowedTagsAdmin(VersionAdmin):
    model = AllowedTags
    list_display = ('content_type',)


class TagAdminInline(GenericTabularInline):
    model = TaggedItem
    extra = 0


class GlossRelationTagAdminInlineForm(ModelForm):
    def __init__(self, *args, **kwargs):
        super(GlossRelationTagAdminInlineForm, self).__init__(*args, **kwargs)
        ct = ContentType.objects.get_for_model(GlossRelation)
        try:
            # Limit choices, try to get allowed tags based on ContentType from AllowedTags.
            self.fields['tag'].queryset = AllowedTags.objects.get(
                content_type=ct).allowed_tags.all()
        except (AttributeError, ObjectDoesNotExist):
            # Get all tags.
            self.fields['tag'].queryset = Tag.objects.all()


class GlossRelationTagAdminInline(TagAdminInline):
    verbose_name = _('Relation type')
    verbose_name_plural = _('Relation types')
    form = GlossRelationTagAdminInlineForm


class GlossRelationAdmin(VersionAdmin):
    raw_id_fields = ('source', 'target',)
    model = GlossRelation
    list_display = ('source', 'tag', 'target',)
    list_filter = ('source__dataset', TagListFilter)
    search_fields = ('source',)
    inlines = [GlossRelationTagAdminInline, ]


class GlossRelationInline(admin.TabularInline):
    model = GlossRelation
    raw_id_fields = ['source', 'target']
    extra = 1
    fk_name = 'source'
    verbose_name = _("Gloss relation")
    verbose_name_plural = _("Gloss relations")


class GlossURLInline(admin.TabularInline):
    model = GlossURL
    extra = 1


class GlossTagInlineForm(ModelForm):
    def __init__(self, *args, **kwargs):
        super(GlossTagInlineForm, self).__init__(*args, **kwargs)
        ct = ContentType.objects.get_for_model(Gloss)
        try:
            # Limit choices, try to get allowed tags based on ContentType from AllowedTags.
            self.fields['tag'].queryset = AllowedTags.objects.get(
                content_type=ct).allowed_tags.all()
        except (AttributeError, ObjectDoesNotExist):
            # Get all tags.
            self.fields['tag'].queryset = Tag.objects.all()


class GlossTagInline(TagAdminInline):
    form = GlossTagInlineForm


class GlossTranslationsInline(admin.TabularInline):
    model = GlossTranslations
    formfield_overrides = {
        models.TextField: {'widget': Textarea(attrs={'rows': 5, 'cols': 50})},
    }
    fields = ('language', 'translations',
              'translations_secondary', 'translations_minor')
    extra = 0

    def has_add_permission(self, request, obj=None):
        # return False
        return True

    def has_delete_permission(self, request, obj=None):
        return False


def publish(modeladmin, request, queryset):
    queryset.update(published=True)


def unpublish(modeladmin, request, queryset):
    queryset.update(published=False)


publish.short_description = _("Publish selected glosses")
unpublish.short_description = _("Unpublish selected glosses")


def exclude_from_ecv(modeladmin, request, queryset):
    queryset.update(exclude_from_ecv=True)


def include_in_ecv(modeladmin, request, queryset):
    queryset.update(exclude_from_ecv=False)


exclude_from_ecv.short_description = _("Exclude glosses from ECV")
include_in_ecv.short_description = _("Include glosses in ECV")


class GlossAdmin(VersionAdmin):
    # Making sure these fields are not edited in admin
    readonly_fields = ('id', 'created_at', 'created_by',
                       'updated_at', 'updated_by', 'concise')
    actions = [publish, unpublish, exclude_from_ecv, include_in_ecv]

    fieldsets = ((None, {'fields': ('dataset', 'assigned_user', 'published', 'exclude_from_ecv', 'id', 'idgloss', 'idgloss_mi', 'wordclasses', 'notes', 'hint', 'signer', 'filmbatch', 'concise',
                                    'lemma', 'nzsl_share_id')},),
                 (_('Created/Updated'), {'fields': ('created_at',
                  'created_by', 'updated_at', 'updated_by')},),
                 (_('Morphology'), {'fields': ('inflection_temporal', 'inflection_manner_degree', 'inflection_plural', 'number_incorporated', 'locatable', 'directional',
                                               'fingerspelling'),
                                    'classes': ('collapse',)}),
                 (_('Phonology'), {'fields': ('handedness', 'location', 'strong_handshape', 'weak_handshape',
                                              'relation_between_articulators', 'absolute_orientation_palm',
                                              'absolute_orientation_fingers', 'relative_orientation_movement',
                                              'relative_orientation_location', 'orientation_change',
                                              'handshape_change', 'repeated_movement', 'alternating_movement',
                                              'movement_shape', 'movement_direction', 'movement_manner', 'contact_type',
                                              'phonology_other', 'mouth_gesture', 'mouthing', 'phonetic_variation',
                                              'one_or_two_hand'),
                                   'classes': ('collapse',)},),
                 (_('Semantics'), {'fields': ('iconic_image', 'named_entity', 'semantic_field'),
                                   'classes': ('collapse',)}),
                 (_('Usage and Variation'), {
                  'fields': ('usage', 'variant_no', 'age_variation',), 'classes': ('collapse',)},),
                 (_('Examples'), {'fields': (
                     ('videoexample1', 'videoexample1_translation'),
                     ('videoexample2', 'videoexample2_translation'),
                     ('videoexample3', 'videoexample3_translation'),
                     ('videoexample4', 'videoexample4_translation'))}),
                 (_('Frequency'), {
                  'fields': ('number_of_occurences',), 'classes': ('collapse',)}),
                 )
    save_on_top = True
    save_as = True
    list_display = ['idgloss', 'dataset',
                    'published', 'exclude_from_ecv', 'idgloss_mi']
    search_fields = ['^idgloss']
    list_filter = ('dataset', 'published', 'exclude_from_ecv',
                   'concise', TagListFilter, )
    inlines = [GlossVideoInline, GlossTranslationsInline, TranslationInline,
               GlossRelationInline, GlossURLInline, GlossTagInline]

    def get_readonly_fields(self, request, obj=None):
        """
        Adds 'published' to 'readonly_fields' if user does not have permission to publish glosses.
        """
        # If obj is not None (and exists), return only the variable 'readonly_fields'
        if obj is None:
            return self.readonly_fields

        # If user doesn't have permission 'dictionary.lock_gloss' add it to readonly_fields
        if not request.user.has_perm('dictionary.publish_gloss'):
            self.readonly_fields += ('publish',)
        return self.readonly_fields

    def save_model(self, request, obj, form, change):
        """Sets created_by and updated_by as the original requests user"""
        obj.created_by = request.user
        obj.updated_by = request.user
        obj.save()

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        form.base_fields['wordclasses'].widget = CheckboxSelectMultiple()
        form.base_fields['usage'].widget = CheckboxSelectMultiple()
        return form

    def save_formset(self, request, form, formset, change):
        """Saves the formsets created_by and updated_by with request.user"""
        if formset.model == Gloss:
            instances = formset.save(commit=False)
            for instance in instances:
                instance.created_by = request.user
                instance.updated_by = request.user
                instance.save()
        else:
            formset.save()


class DialectInline(admin.TabularInline):
    model = Dialect
    extra = 0


class LanguageAdmin(VersionAdmin, ModelTranslationAdmin):
    model = Language


class SignLanguageAdmin(VersionAdmin, ModelTranslationAdmin):
    model = SignLanguage
    inlines = [DialectInline]


class ShareValidationAggregationAdmin(VersionAdmin):
    models = ShareValidationAggregation
    list_display = ('gloss', 'agrees', 'disagrees')


class FieldChoiceAdmin(admin.ModelAdmin):
    model = FieldChoice
    list_display = ('field', 'english_name', 'machine_value',)


class AssignedGlossInline(admin.StackedInline):
    model = Gloss
    fk_name = 'assigned_user'
    extra = 0
    readonly_fields = ('idgloss',)
    can_delete = False
    verbose_name_plural = "Assigned glosses"
    fieldsets = ((None, {'fields': ('idgloss',)}),)

    def has_add_permission(self, request, obj=None):
        return False


class InputFilter(admin.SimpleListFilter):
    template = 'admin/input_filter.html'

    def lookups(self, request, model_admin):
        # Dummy, required to show the filter.
        return ((),)

    def choices(self, changelist):
        # Grab only the "all" option.
        all_choice = next(super().choices(changelist))
        all_choice['query_parts'] = (
            (k, v)
            for k, v in changelist.get_filters_params().items()
            if k != self.parameter_name
        )
        yield all_choice


class GlossFilter(InputFilter):
    parameter_name = 'gloss'
    title = _('Gloss')

    def queryset(self, request, queryset):
        if self.value() is not None:
            gloss = self.value()

            return queryset.filter(
                models.Q(gloss__idgloss__contains=gloss)
            )


class ManualValidationAggregationAdmin(admin.ModelAdmin):
    model = ManualValidationAggregation
    list_display = ("gloss", "group", "sign_seen_yes", "sign_seen_no", "sign_seen_not_sure")
    search_fields = ["group"]
    list_filter = [GlossFilter, "group"]


class ValidationRecordAdmin(admin.ModelAdmin):
    model = ValidationRecord
    list_display = ("gloss", "response_id", "sign_seen")
    search_fields = ["response_id"]
    list_filter = [GlossFilter, "sign_seen"]


class UserAdmin(AuthUserAdmin):
    inlines = [AssignedGlossInline]


admin.site.register(Language, LanguageAdmin)
admin.site.register(SignLanguage, SignLanguageAdmin)
admin.site.register(Gloss, GlossAdmin)
admin.site.register(Translation, TranslationAdmin)
admin.site.register(Dataset, DatasetAdmin)
admin.site.register(GlossRelation, GlossRelationAdmin)
admin.site.register(AllowedTags, AllowedTagsAdmin)
admin.site.register(Lemma)
admin.site.register(ShareValidationAggregation, ShareValidationAggregationAdmin)
admin.site.register(ManualValidationAggregation, ManualValidationAggregationAdmin)
admin.site.register(ValidationRecord, ValidationRecordAdmin)

admin.site.unregister(User)
admin.site.register(User, UserAdmin)

# The following models have been removed from the admin because they are not used at the moment.
admin.site.register(FieldChoice, FieldChoiceAdmin)
# admin.site.register(MorphologyDefinition)
