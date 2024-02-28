# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import csv
import datetime
import io

from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth.models import AnonymousUser, Permission, User
from django.contrib.contenttypes.models import ContentType
from django.contrib.sites.models import Site
from django.test import Client, TestCase
from django.urls import reverse
from django.utils.timezone import get_current_timezone
from django_comments.models import Comment
from guardian.shortcuts import assign_perm
from tagging.models import Tag

from signbank.dictionary.models import (
    Dataset,
    FieldChoice,
    Gloss,
    GlossTranslations,
    Language,
    SignLanguage, ShareValidationAggregation, ValidationRecord
)
from signbank.video.models import GlossVideo, GlossVideoToken


class GlossListViewTestCase(TestCase):
    def setUp(self):
        # Create user and add permissions
        self.user = User.objects.create_user(username="test", email=None, password="test")
        permission = Permission.objects.get(codename='search_gloss')
        self.user.user_permissions.add(permission)
        self.user.save()
        # Create client for user with permission
        self.client = Client()
        self.client.force_login(self.user)

        # Create user with no permission
        self.user_noperm = User.objects.create_user(username="noperm", email=None, password="noperm")

        # Create client for user with no permission
        self.client_noperm = Client()
        self.client_noperm.force_login(self.user_noperm)

    def test_get_user_not_authenticated(self):
        """Test that non-authenticated user can't access the search page via GET."""
        self.client.logout()
        response = self.client.get(reverse('dictionary:admin_gloss_list'))
        self.assertFalse(response.status_code == 200)

    def test_get_user_authenticated_has_permission(self):
        """Tests that an authenticated user with proper permissions can access search page via GET."""
        response = self.client.get(reverse('dictionary:admin_gloss_list'))
        self.assertTrue(response.status_code == 200)

    def test_get_user_authenticated_no_permission(self):
        """Tests that authenticated user without proper permission can't access search page via GET."""
        # Using client_noperm
        response = self.client_noperm.get(reverse('dictionary:admin_gloss_list'))
        self.assertFalse(response.status_code == 200)
        # 302 Found
        self.assertTrue(response.status_code == 302)

    def test_get_csv(self):
        """Tests that a CSV file can be successfully downloaded without filters applied"""
        permission = Permission.objects.get(codename='export_csv')
        self.user.user_permissions.add(permission)
        self.user.save()
        response = self.client.get(reverse('dictionary:admin_gloss_list'), { 'format': 'CSV-standard' })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['Content-Type'], 'text/csv; charset=utf-8')
        self.assertEqual(response.headers['Content-Disposition'], 'attachment; filename="dictionary-export.csv"')

    def test_get_ready_for_validation_csv(self):
        """
        Tests that a CSV file can be successfully downloaded containing glosses that are
        tagged ready for validation
        """
        csv_permission = Permission.objects.get(codename="export_csv")
        self.user.user_permissions.add(csv_permission)

        signlanguage = SignLanguage.objects.create(
            pk=2, name="testsignlanguage", language_code_3char="tst"
        )
        dataset = Dataset.objects.create(
            name="testdataset", signlanguage=signlanguage
        )
        assign_perm("dictionary.view_dataset", self.user, dataset)

        testgloss = Gloss.objects.create(
            idgloss="testgloss", dataset=dataset, created_by=self.user, updated_by=self.user
        )
        Tag.objects.add_tag(testgloss, settings.TAG_READY_FOR_VALIDATION)

        language_en = Language.objects.create(
            name="English", language_code_2char="EN", language_code_3char="ENG"
        )
        translation = GlossTranslations.objects.create(
            gloss=testgloss, language=language_en, translations="test gloss"
        )

        validation_video_type = FieldChoice.objects.get(
            field="video_type", english_name="validation"
        )
        testfile = SimpleUploadedFile(
            "testvid.mp4", b'data \x00\x01', content_type="video/mp4")
        glossvid = GlossVideo.objects.create(
            gloss=testgloss,
            is_public=False,
            dataset=testgloss.dataset,
            videofile=testfile,
            video_type=validation_video_type,
        )

        tag_id = Tag.objects.filter(name=settings.TAG_READY_FOR_VALIDATION).values_list("pk", flat=True)[0]

        response = self.client.get(
            reverse("dictionary:admin_gloss_list"),
            {"format": "CSV-ready-for-validation", "tags": tag_id},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/csv; charset=utf-8")
        self.assertEqual(
            response.headers["Content-Disposition"],
            'attachment; filename="ready-for-validation-export.csv"',
        )

        content = response.content.decode('utf-8')
        csv_reader = csv.reader(io.StringIO(content))
        csv_content = list(csv_reader)
        self.assertEqual(len(csv_content), 2)

        video_tokens = GlossVideoToken.objects.filter(video=glossvid)
        self.assertEqual(video_tokens.count(), 1)
        video_token = video_tokens.get()
        headers = csv_content[0]
        body = csv_content[1]
        self.assertEqual(["idgloss", "gloss_main", "video_url"], headers)
        self.assertEqual(testgloss.idgloss, body[0])
        self.assertEqual(translation.translations, body[1])
        # video url changes between environments, so only checking it's not empty
        partial_url = reverse("video:get_signed_glossvideo_url",
                              kwargs={"token": video_token.token, "videoid": glossvid.pk})
        self.assertIn(partial_url, body[2])

    def test_get_validation_results_csv(self):
        """
        Tests that a CSV file can be successfully downloaded containing glosses that are
        tagged validation:check-results and their validation results
        """
        csv_permission = Permission.objects.get(codename="export_csv")
        self.user.user_permissions.add(csv_permission)

        gloss_content_type = ContentType.objects.get_for_model(Gloss)
        site = Site.objects.get_current()
        submit_date = datetime.datetime.now(tz=get_current_timezone())
        signlanguage = SignLanguage.objects.create(
            pk=2, name="testsignlanguage", language_code_3char="tst"
        )
        dataset = Dataset.objects.create(
            name="testdataset", signlanguage=signlanguage
        )
        assign_perm("dictionary.view_dataset", self.user, dataset)

        testgloss_1 = Gloss.objects.create(
            idgloss="testgloss:1", dataset=dataset, created_by=self.user, updated_by=self.user
        )
        Tag.objects.add_tag(testgloss_1, settings.TAG_VALIDATION_CHECK_RESULTS)
        vr1_g1 = ValidationRecord.objects.create(
            gloss=testgloss_1,
            sign_seen=ValidationRecord.SignSeenChoices.YES.value,
            response_id="Response_1",
            respondent_first_name="John",
            respondent_last_name="Doe",
            comment=""
        )
        vr2_g1 = ValidationRecord.objects.create(
            gloss=testgloss_1,
            sign_seen=ValidationRecord.SignSeenChoices.NO.value,
            response_id="Response_2",
            respondent_first_name="Jane",
            respondent_last_name="Doe",
            comment="Cool use"
        )
        vr3_g1 = ValidationRecord.objects.create(
            gloss=testgloss_1,
            sign_seen=ValidationRecord.SignSeenChoices.NOT_SURE.value,
            response_id="Response_3",
            respondent_first_name="First",
            respondent_last_name="Last",
            comment="Similar to test gloss"
        )
        ShareValidationAggregation.objects.create(
            gloss=testgloss_1,
            agrees=3,
            disagrees=7
        )
        c1_g1 = Comment.objects.create(
            content_type=gloss_content_type,
            object_pk=str(testgloss_1.pk),
            is_public=False,
            site=site,
            submit_date=submit_date,
            user_name="Anonymous",
            comment="Disagree with this",
        )
        c2_g1 = Comment.objects.create(
            content_type=gloss_content_type,
            object_pk=str(testgloss_1.pk),
            is_public=False,
            site=site,
            submit_date=submit_date,
            user_name="Sallymil",
            comment="Too complicated"
        )

        testgloss_2 = Gloss.objects.create(
            idgloss="testgloss:2", dataset=dataset, created_by=self.user, updated_by=self.user
        )
        Tag.objects.add_tag(testgloss_2, settings.TAG_VALIDATION_CHECK_RESULTS)
        vr1_g2 = ValidationRecord.objects.create(
            gloss=testgloss_2,
            sign_seen=ValidationRecord.SignSeenChoices.YES.value,
            response_id="Response_1",
            respondent_first_name="John",
            respondent_last_name="Doe",
            comment=""
        )
        vr2_g2 = ValidationRecord.objects.create(
            gloss=testgloss_2,
            sign_seen=ValidationRecord.SignSeenChoices.NOT_SURE.value,
            response_id="Response_2",
            respondent_first_name="Jane",
            respondent_last_name="Doe",
            comment="Don't like hand movement"
        )
        vr3_g2 = ValidationRecord.objects.create(
            gloss=testgloss_2,
            sign_seen=ValidationRecord.SignSeenChoices.NO.value,
            response_id="Response_3",
            respondent_first_name="First",
            respondent_last_name="Last",
            comment=""
        )
        ShareValidationAggregation.objects.create(
            gloss=testgloss_2,
            agrees=7,
            disagrees=3
        )
        c1_g2 = Comment.objects.create(
            content_type=gloss_content_type,
            object_pk=str(testgloss_2.pk),
            is_public=False,
            site=site,
            submit_date=submit_date,
            user_name="Anonymous",
            comment="Duplicate to gloss test"
        )
        c2_g2 = Comment.objects.create(
            content_type=gloss_content_type,
            object_pk=str(testgloss_2.pk),
            is_public=False,
            site=site,
            submit_date=submit_date,
            user_name="Sallymil",
            comment="Funny word"
        )

        tag_id = \
            Tag.objects.filter(name=settings.TAG_VALIDATION_CHECK_RESULTS).values_list("pk",
                                                                                       flat=True)[
                0]

        response = self.client.get(
            reverse("dictionary:admin_gloss_list"),
            {"format": "CSV-validation-results", "tags": tag_id},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/csv; charset=utf-8")
        self.assertEqual(
            response.headers["Content-Disposition"],
            'attachment; filename="validation-results-export.csv"',
        )

        content = response.content.decode('utf-8')
        cvs_reader = csv.reader(io.StringIO(content))
        body = list(cvs_reader)
        self.assertEqual(len(body), 3)

        headers = body[0]
        self.assertEqual(
            ["idgloss",
             "have seen sign - yes",
             "have seen sign - no",
             "have seen sign - not sure",
             "total",
             "comments"],
            headers
        )
        gloss_1_response = body[1]
        gloss_2_response = body[2]
        self.assertEqual(testgloss_1.idgloss, gloss_1_response[0])
        self.assertEqual("4", gloss_1_response[1])  # have seen sign - yes
        self.assertEqual("8", gloss_1_response[2])  # have seen sign - no
        self.assertEqual("1", gloss_1_response[3])  # have seen sign - not sure
        self.assertEqual("13", gloss_1_response[4])  # total
        # comments
        self.assertNotIn(
            f"{vr1_g1.respondent_first_name} {vr1_g1.respondent_last_name}: {vr1_g1.comment}",
            gloss_1_response[5])
        self.assertIn(
            f"{vr2_g1.respondent_first_name} {vr2_g1.respondent_last_name}: {vr2_g1.comment}",
            gloss_1_response[5])
        self.assertIn(
            f"{vr3_g1.respondent_first_name} {vr3_g1.respondent_last_name}: {vr3_g1.comment}",
            gloss_1_response[5])
        self.assertIn(f"{c1_g1.user_name}: {c1_g1.comment}", gloss_1_response[5])
        self.assertIn(f"{c2_g1.user_name}: {c2_g1.comment}", gloss_1_response[5])

        self.assertEqual(testgloss_2.idgloss, gloss_2_response[0])
        self.assertEqual("8", gloss_2_response[1])  # have seen sign - yes
        self.assertEqual("4", gloss_2_response[2])  # have seen sign - no
        self.assertEqual("1", gloss_2_response[3])  # have seen sign - not sure
        self.assertEqual("13", gloss_2_response[4])  # total
        # comments
        self.assertNotIn(
            f"{vr1_g2.respondent_first_name} {vr1_g2.respondent_last_name}: {vr1_g2.comment}",
            gloss_2_response[5])
        self.assertIn(
            f"{vr2_g2.respondent_first_name} {vr2_g2.respondent_last_name}: {vr2_g2.comment}",
            gloss_2_response[5])
        self.assertNotIn(
            f"{vr3_g2.respondent_first_name} {vr3_g2.respondent_last_name}: {vr3_g2.comment}",
            gloss_2_response[5])
        self.assertIn(f"{c1_g2.user_name}: {c1_g2.comment}", gloss_2_response[5])
        self.assertIn(f"{c2_g2.user_name}: {c2_g2.comment}", gloss_2_response[5])

    def test_post(self):
        """Testing that the search page can't be accessed with POST."""
        response = self.client.post(reverse('dictionary:admin_gloss_list'))
        # 405 Method Not Allowed
        self.assertTrue(response.status_code == 405)

    def test_put(self):
        """Tests that PUT doesn't work on search page."""
        response = self.client.put(reverse('dictionary:admin_gloss_list'))
        # 405 Method Not Allowed
        self.assertTrue(response.status_code == 405)

    def test_delete(self):
        """Tests that DELETE doesn't work on search page."""
        response = self.client.delete(reverse('dictionary:admin_gloss_list'))
        # 405 Method Not Allowed
        self.assertTrue(response.status_code == 405)


class TestValidationResultsView(TestCase):
    def setUp(self):
        # Create user and add permissions
        self.user = User.objects.create_user(username="test", email=None, password="test",
                                             is_staff=True)
        permission = Permission.objects.get(codename='search_gloss')
        self.user.user_permissions.add(permission)

        self.client = Client()
        self.client.force_login(self.user)

        # Create a gloss
        # Migrations have id=1 already
        self.signlanguage = SignLanguage.objects.create(pk=2, name="testsignlanguage",
                                                        language_code_3char="tst")
        self.dataset = Dataset.objects.create(name="testdataset", signlanguage=self.signlanguage)
        self.gloss = Gloss.objects.create(idgloss="testgloss", dataset=self.dataset)

        self.share_validation_aggregation_1 = ShareValidationAggregation.objects.create(
            gloss=self.gloss, agrees=2, disagrees=5)
        self.share_validation_aggregation_2 = ShareValidationAggregation.objects.create(
            gloss=self.gloss, agrees=5, disagrees=2)
        self.validation_record = ValidationRecord.objects.create(gloss=self.gloss,
            response_id="R_bogus", sign_seen=ValidationRecord.SignSeenChoices.YES,
            comment="A comment")
        gloss_content_type = ContentType.objects.get_for_model(Gloss)
        site = Site.objects.get_current()
        self.share_comment = Comment.objects.create(content_type=gloss_content_type,
            object_pk=self.gloss.pk, user_name="test_user", comment="Another comment", site=site,
            is_public=False, submit_date=datetime.datetime.now(tz=get_current_timezone()))
        # Assign view permissions to dataset for user
        assign_perm('view_dataset', self.user, self.dataset)

    def test_validation_results_in_context_multiple_share_results(self):
        response = self.client.get(
            reverse("dictionary:admin_gloss_view", kwargs={"pk": self.gloss.pk}))
        self.assertEqual(response.status_code, 200)
        self.assertListEqual(list(response.context["share_comments"]), [self.share_comment])
        self.assertListEqual(list(response.context["validation_records"]),
                             [self.validation_record])
        self.assertDictEqual(response.context["share_validation_aggregation"],
                             {"agrees": 7, "disagrees": 7})
        self.assertEqual(response.context["sign_seen_yes"], 1)
        self.assertEqual(response.context["sign_seen_no"], 0)
        self.assertEqual(response.context["sign_seen_maybe"], 0)

    def test_validation_results_in_context_single_share_results(self):
        ShareValidationAggregation.objects.filter(gloss=self.gloss).last().delete()
        response = self.client.get(
            reverse("dictionary:admin_gloss_view", kwargs={"pk": self.gloss.pk}))
        self.assertEqual(response.status_code, 200)
        self.assertListEqual(list(response.context["share_comments"]), [self.share_comment])
        self.assertListEqual(list(response.context["validation_records"]),
                             [self.validation_record])
        self.assertDictEqual(response.context["share_validation_aggregation"],
                             {"agrees": 2, "disagrees": 5})
        self.assertEqual(response.context["sign_seen_yes"], 1)
        self.assertEqual(response.context["sign_seen_no"], 0)
        self.assertEqual(response.context["sign_seen_maybe"], 0)

    def test_validation_results_in_context_no_share_results(self):
        ShareValidationAggregation.objects.filter(gloss=self.gloss).delete()
        response = self.client.get(
            reverse("dictionary:admin_gloss_view", kwargs={"pk": self.gloss.pk}))
        self.assertEqual(response.status_code, 200)
        self.assertListEqual(list(response.context["share_comments"]), [self.share_comment])
        self.assertListEqual(list(response.context["validation_records"]),
                             [self.validation_record])
        self.assertDictEqual(response.context["share_validation_aggregation"],
                             {"agrees": 0, "disagrees": 0})
        self.assertEqual(response.context["sign_seen_yes"], 1)
        self.assertEqual(response.context["sign_seen_no"], 0)
        self.assertEqual(response.context["sign_seen_maybe"], 0)
