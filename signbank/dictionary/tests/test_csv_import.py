# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import copy
import csv
import random
import uuid
from unittest import mock

from django.conf import settings
from django.contrib.auth.models import User, Permission
from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase
from django.urls import reverse
from django_comments import get_model as comments_get_model
from guardian.shortcuts import assign_perm
from tagging.models import Tag, TaggedItem

from signbank.dictionary.models import (
    SignLanguage, Dataset, FieldChoice, Gloss, Language,
    ManualValidationAggregation, ValidationRecord)
from signbank.video.models import GlossVideo


class ShareCSVImportTestCase(TestCase):
    def setUp(self):
        # Create user and add permissions
        self.user = User.objects.create_user(username="test", email=None, password="test")
        csv_permission = Permission.objects.get(codename='import_csv')
        self.user.user_permissions.add(csv_permission)

        # Create client with change_gloss permission.
        self.client = Client()
        self.client.force_login(self.user)

        # Create user with no permissions
        self.user_noperm = User.objects.create_user(username="noperm", email=None,
                                                    password="noperm")
        self.client_noperm = Client()
        self.client_noperm.force_login(self.user_noperm)

        # Create client not logged in
        self.client_nologin = Client()

        # Create a gloss
        # Migrations have id=1 already
        self.signlanguage = SignLanguage.objects.create(pk=2, name="testsignlanguage",
                                                        language_code_3char="tst")
        self.dataset = Dataset.objects.create(name="testdataset", signlanguage=self.signlanguage)
        self.language_en = Language.objects.create(name='English', language_code_2char='en',
                                                   language_code_3char='eng')
        self.language_mi = Language.objects.create(name="Māori", language_code_2char="mi",
                                                   language_code_3char="mri")
        FieldChoice.objects.create(field="video_type", english_name="validation",
                                   machine_value=random.randint(0, 99999))
        FieldChoice.objects.create(field="semantic_field", english_name="Test",
                                   machine_value=random.randint(0, 99999))
        FieldChoice.objects.create(field="semantic_field", english_name="Miscellaneous",
                                   machine_value=random.randint(0, 99999))

        # Assign view permissions to dataset for user
        assign_perm('view_dataset', self.user, self.dataset)

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
        "videos": "/VID_20170815_153446275.mp4",
        "illustrations": "/kiwifruit-2-6422.png",
        "usage_examples": "/fire.1923.finalexample1.mb.r480x360.mp4",
        "sign_comments": (
            "contribution_limit_test_1: Comment 0|Comment 33"
        )
    }

    def test_import_view_post_with_no_permission(self):
        """Test that you get 302 Found or 403 Forbidden if you try without csv import permission."""
        response = self.client_noperm.post(reverse('dictionary:import_nzsl_share_gloss_csv'))
        # Make sure user does not have change_gloss permission.
        self.assertFalse(response.wsgi_request.user.has_perm('dictionary.import_csv'))
        # Should return 302 Found, or 403 Forbidden
        self.assertIn(response.status_code, [403, 302])

    def test_import_view_post_nologin(self):
        """Testing POST with anonymous user."""
        response = self.client_nologin.post(reverse('dictionary:import_nzsl_share_gloss_csv'))
        # Should return 302 Found, or 403 Forbidden
        self.assertIn(response.status_code, [302, 403])

    def test_import_view_no_post_method(self):
        """Test that using GET re-renders import view"""
        response = self.client.get(reverse('dictionary:import_nzsl_share_gloss_csv'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.request["PATH_INFO"],
                         reverse('dictionary:import_nzsl_share_gloss_csv'))

    def test_import_view_successful_file_upload(self):
        """Test a csv file can successfully be read by the NZSLShare csv import view"""
        file_name = "test.csv"
        csv_content = self._csv_content
        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(csv_content.keys())
            writer.writerow(csv_content.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )
        response = self.client.post(
            reverse('dictionary:import_nzsl_share_gloss_csv'),
            {"dataset": self.dataset.pk, "file": file},
            format="multipart"
        )
        self.assertEqual(response.status_code, 200)
        session = self.client.session
        self.assertEqual(self.dataset.pk, session["dataset_id"])
        self.assertListEqual([self._csv_content], session["glosses_new"])

    def test_share_ids_existing_on_glosses_with_videos_are_skipped(self):
        """
        Test a csv file row, for which an existing gloss has the share id associated
        with it, is skipped, so long as it has a video (glosses with videos may not
        be re-imported)
        """
        file_name = "test.csv"
        csv_content = [copy.deepcopy(self._csv_content), copy.deepcopy(self._csv_content)]
        csv_content[1]["id"] = "12345"
        
        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(csv_content[0].keys())
            for row in csv_content:
                writer.writerow(row.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )
        gloss = Gloss.objects.create(dataset=self.dataset, idgloss="Share:11", nzsl_share_id="12345")
        GlossVideo.objects.create(
            gloss=gloss,
            is_public=True,
            dataset=Dataset.objects.create(name="testdataset2", signlanguage=self.signlanguage),
            videofile=SimpleUploadedFile("testvid.mp4", b'data \x00\x01', content_type="video/mp4"),
            video_type=FieldChoice.objects.first(),
            title="Main"
        )

        response = self.client.post(
            reverse('dictionary:import_nzsl_share_gloss_csv'),
            {"dataset": self.dataset.pk, "file": file},
            format="multipart"
        )
        self.assertEqual(response.status_code, 200)
        session = self.client.session
        self.assertEqual(self.dataset.pk, session["dataset_id"])
        self.assertListEqual([csv_content[0]], session["glosses_new"])
        self.assertListEqual([csv_content[1]], response.context["skipped_existing_glosses"])

    def test_share_ids_existing_on_glosses_with_no_videos_have_their_videos_reimported(self):
        """
        Test a csv file row, for which an existing gloss has the share id associated with it,
        is not skipped if it has no video associated (glosses without videos may be re-imported)
        """
        file_name = "test.csv"
        csv_content = [copy.deepcopy(self._csv_content), copy.deepcopy(self._csv_content)]
        csv_content[1]["id"] = "12345"

        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(csv_content[0].keys())
            for row in csv_content:
                writer.writerow(row.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )
        gloss = Gloss.objects.create(dataset=self.dataset, idgloss="Share:11", nzsl_share_id="12345")
        response = self.client.post(
            reverse('dictionary:import_nzsl_share_gloss_csv'),
            {"dataset": self.dataset.pk, "file": file},
            format="multipart"
        )
        self.assertEqual(response.status_code, 200)
        session = self.client.session
        self.assertEqual(self.dataset.pk, session["dataset_id"])
        with self.assertRaises(AssertionError):
            self.assertListEqual([csv_content[0]], session["glosses_new"])
        with self.assertRaises(AssertionError):
            self.assertListEqual([csv_content[1]], response.context["skipped_existing_glosses"])

    def test_duplicate_share_ids_existing_on_glosses_with_no_videos_are_skipped(self):
        """
        Test a csv file row. If there is more than one existing gloss matching the
        nzsl_share_id, gracefully skip the row regardless of whether the glosses have
        videos or not.
        """
        file_name = "test.csv"
        csv_content = [copy.deepcopy(self._csv_content), copy.deepcopy(self._csv_content)]
        csv_content[1]["id"] = "12345"

        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(csv_content[0].keys())
            for row in csv_content:
                writer.writerow(row.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )
        # Same nzsl_share_id
        Gloss.objects.create(dataset=self.dataset, idgloss="Share:11", nzsl_share_id="12345")
        Gloss.objects.create(dataset=self.dataset, idgloss="Share:12", nzsl_share_id="12345")
        response = self.client.post(
            reverse('dictionary:import_nzsl_share_gloss_csv'),
            {"dataset": self.dataset.pk, "file": file},
            format="multipart"
        )
        self.assertEqual(response.status_code, 200)
        session = self.client.session
        self.assertEqual(self.dataset.pk, session["dataset_id"])
        self.assertListEqual([csv_content[0]], session["glosses_new"])
        self.assertListEqual([csv_content[1]], response.context["skipped_existing_glosses"])

    def test_confirmation_view_confirm_gloss_creation(self):
        """
        Test that the confirm NZSLShare import csv view can successfully create a gloss and
        related entities (except videos)
        """
        share_importer = User.objects.get(
            username="nzsl_share_importer",
            first_name="Importer",
            last_name="NZSL Share",
        )
        share_tag = Tag.objects.get(name="nzsl-share")
        not_public_tag = Tag.objects.create(name="not public")

        csv_content = self._csv_content
        glosses = [csv_content]
        s = self.client.session
        s.update({
            "dataset_id": self.dataset.pk,
            "glosses_new": glosses
        })
        s.save()
        with mock.patch(
                'signbank.dictionary.csv_import.retrieve_videos_for_glosses') as mock_tasks:
            mock_tasks.return_value = None
            response = self.client.post(
                reverse("dictionary:confirm_import_nzsl_share_gloss_csv"),
                {"confirm": True}
            )
            mock_tasks.assert_called_once()
        self.assertEqual(response.status_code, 200)

        maori_words = csv_content['maori'].split(', ')

        # check the details of the gloss
        gloss_qs = Gloss.objects.filter(dataset=self.dataset,
                                        idgloss__contains=csv_content["word"])
        self.assertTrue(gloss_qs.count(), 1)
        gloss = gloss_qs.first()
        self.assertEqual(f"{csv_content['word']}:{gloss.pk}", gloss.idgloss)
        self.assertEqual(f"{maori_words[0]}:{gloss.pk}", gloss.idgloss_mi)
        self.assertEqual("", gloss.notes)
        self.assertEqual(share_importer, gloss.created_by)
        self.assertEqual(share_importer, gloss.updated_by)
        self.assertEqual(csv_content["contributor_username"], gloss.signer.english_name)
        self.assertTrue(gloss.exclude_from_ecv)
        self.assertIsNone(gloss.assigned_user)

        # check the semantic fields for the gloss
        # Test Topic does not exist, so instead one topic should be miscellaneous
        semantic_fields = gloss.semantic_field.all()
        self.assertEqual(semantic_fields.count(), 2)
        self.assertTrue(semantic_fields.filter(english_name="Miscellaneous").exists())
        self.assertTrue(semantic_fields.filter(english_name="Test").exists())

        # check the glosstranslations for the gloss
        translations = gloss.glosstranslations_set.all()
        self.assertEqual(translations.count(), 2)
        eng = translations.get(language=self.language_en)
        mi = translations.get(language=self.language_mi)
        self.assertEqual(csv_content["word"], eng.translations)
        self.assertEqual(csv_content["secondary"], eng.translations_secondary)
        self.assertEqual(maori_words[0], mi.translations)
        self.assertEqual(", ".join(maori_words[1:]), mi.translations_secondary)

        # Check the comments created for the gloss
        comments = comments_get_model().objects.filter(object_pk=str(gloss.id))
        self.assertEqual(comments.count(), 3)
        self.assertTrue(comments.filter(
            user_name=csv_content["contributor_username"],
            comment=csv_content["notes"]
        ).exists())
        self.assertTrue(comments.filter(
            user_name="contribution_limit_test_1",
            comment=" Comment 0"
        ).exists())
        self.assertTrue(comments.filter(
            user_name="Unknown",
            comment="Comment 33"
        ).exists())

        share_validation_aggregations = gloss.share_validation_aggregations.all()
        self.assertEqual(share_validation_aggregations.count(), 1)
        share_validation_aggregation = share_validation_aggregations.get()
        self.assertEqual(share_validation_aggregation.agrees, 0)
        self.assertEqual(share_validation_aggregation.disagrees, 1)

        tagged_glosses = TaggedItem.objects.get_intersection_by_model(
            gloss_qs, [not_public_tag, share_tag]
        )
        self.assertQuerysetEqual(tagged_glosses, gloss_qs)

        # There should be no gloss videos at this point because we have mocked the task to
        # create them
        self.assertEqual(gloss.glossvideo_set.count(), 0)

    def test_confirmation_view_cancel_gloss_creation(self):
        csv_content = self._csv_content
        glosses = [csv_content]
        s = self.client.session
        s.update({
            "dataset_id": self.dataset.pk,
            "glosses_new": glosses
        })
        s.save()

        response = self.client.post(
            reverse("dictionary:confirm_import_nzsl_share_gloss_csv"),
            {"cancel": True}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse("dictionary:import_nzsl_share_gloss_csv"))
        new_session = self.client.session
        self.assertNotIn("dataset_id", new_session.keys())
        self.assertNotIn("glosses_new", new_session.keys())

    def test_confirmation_view_no_post_method(self):
        """Test that using GET redirects to import view"""
        response = self.client.get(reverse('dictionary:confirm_import_nzsl_share_gloss_csv'))
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse("dictionary:import_nzsl_share_gloss_csv"))


class QualtricsCSVImportTestCase(TestCase):
    def setUp(self):
        # Create user and add permissions
        self.user = User.objects.create_user(username="test", email=None, password="test")
        csv_permission = Permission.objects.get(codename='import_csv')
        self.user.user_permissions.add(csv_permission)

        # Create client with change_gloss permission.
        self.client = Client()
        self.client.force_login(self.user)

        # Create user with no permissions
        self.user_noperm = User.objects.create_user(username="noperm", email=None,
                                                    password="noperm")
        self.client_noperm = Client()
        self.client_noperm.force_login(self.user_noperm)

        # Create client not logged in
        self.client_nologin = Client()

        # Create a gloss
        # Migrations have id=1 already
        self.signlanguage = SignLanguage.objects.create(pk=2, name="testsignlanguage",
                                                        language_code_3char="tst")
        self.dataset = Dataset.objects.create(name="testdataset", signlanguage=self.signlanguage)
        validation_video_type = FieldChoice.objects.get(
            field="video_type", english_name="validation"
        )

        self.gloss_1 = Gloss.objects.create(idgloss="testgloss:1", dataset=self.dataset)
        testfile = SimpleUploadedFile(
            "testvid.mp4", b'data \x00\x01', content_type="video/mp4")
        self.glossvideo_1 = GlossVideo.objects.create(
            gloss=self.gloss_1,
            is_public=True,
            dataset=self.gloss_1.dataset,
            videofile=testfile,
            video_type=validation_video_type,
            title="Main"
        )
        Tag.objects.add_tag(self.gloss_1, settings.TAG_READY_FOR_VALIDATION)
        self.gloss_2 = Gloss.objects.create(idgloss="testgloss:2", dataset=self.dataset)
        testfile_2 = SimpleUploadedFile(
            "testvid.mp4", b'data \x00\x01', content_type="video/mp4")
        self.glossvideo_2 = GlossVideo.objects.create(
            gloss=self.gloss_2,
            is_public=True,
            dataset=self.gloss_2.dataset,
            videofile=testfile_2,
            video_type=validation_video_type,
            title="Main"
        )
        Tag.objects.add_tag(self.gloss_2, settings.TAG_READY_FOR_VALIDATION)

        # Assign view permissions to dataset for user
        assign_perm('view_dataset', self.user, self.dataset)

    # Unimportant columns are excluded from csv
    _csv_headers = [
        "Status",
        "ResponseId",
        "RecipientLastName",
        "RecipientFirstName",
        "1_Q1_1",
        "1_Q2",
        "1_Q2_5_TEXT",
        "2_Q1_1",
        "2_Q2",
        "2_Q2_5_TEXT",
        "3_Q1_1",
        "3_Q2",
        "3_Q2_5_TEXT"
    ]
    _csv_content = [
        # row 2 of file, contains urls with gloss pks
        {
            "Status": "IP Address",
            "ResponseId": "R_4PuIGsoEF7g76aE",
            "RecipientLastName": "Doe",
            "RecipientFirstName": "John",
            "1_Q1_1": f"unimportant_text/video/signed_url/{uuid.uuid4()}/1/ - Have seen this myself",
            "1_Q2": "",
            "1_Q2_5_TEXT": "",
            "2_Q1_1": f"unimportant_text/video/signed_url/{uuid.uuid4()}/2/ - Have seen this myself",
            "2_Q2": "",
            "2_Q2_5_TEXT": "comment",
            "3_Q1_1": f"unimportant_text/video/signed_url/{uuid.uuid4()}/3/ - Have seen this myself",
            "3_Q2": "",
            "3_Q2_5_TEXT": ""
        },
        # row 3, will be ignored
        {
            "Status": "IP Address",
            "ResponseId": "R_4PuIGsoEF7g76aE",
            "RecipientLastName": "Doe",
            "RecipientFirstName": "John",
            "1_Q1_1": "Yes",
            "1_Q2": "",
            "1_Q2_5_TEXT": "",
            "2_Q1_1": "",
            "2_Q2": "",
            "2_Q2_5_TEXT": "",
            "3_Q1_1": "",
            "3_Q2": "",
            "3_Q2_5_TEXT": ""
        },
        # responses start here
        {
            "Status": "IP Address",
            "ResponseId": "R_4PuIGsoEF7g76aE",
            "RecipientLastName": "Doe",
            "RecipientFirstName": "John",
            "1_Q1_1": "Yes",
            "1_Q2": "",
            "1_Q2_5_TEXT": "",
            "2_Q1_1": "No",
            "2_Q2": "",
            "2_Q2_5_TEXT": "comment",
            "3_Q1_1": "not sure ",
            "3_Q2": "",
            "3_Q2_5_TEXT": ""
        },
        {
            "Status": "Imported",
            "ResponseId": "R_4nejxM9PFHp9JBL",
            "RecipientLastName": "Doe",
            "RecipientFirstName": "Jane",
            "1_Q1_1": "No",
            "1_Q2": "Write a comment",
            "1_Q2_5_TEXT": "Test Comment",
            "2_Q1_1": "No",
            "2_Q2": "",
            "2_Q2_5_TEXT": "comment",
            "3_Q1_1": "not sure ",
            "3_Q2": "",
            "3_Q2_5_TEXT": ""
        },
        {
            "Status": "IP Address",
            "ResponseId": "R_4wMijsb0UrE6SQy",
            "RecipientLastName": "Last",
            "RecipientFirstName": "First",
            "1_Q1_1": "Not sure ",
            "1_Q2": "Write a comment,I want to talk about this sign in NZSL - contact me",
            "1_Q2_5_TEXT": "Test Comment",
            "2_Q1_1": "No",
            "2_Q2": "",
            "2_Q2_5_TEXT": "comment",
            "3_Q1_1": "not sure ",
            "3_Q2": "",
            "3_Q2_5_TEXT": ""
        },
        # response will be skipped / status mismatch
        {
            "Status": "Spam",
            "ResponseId": "R_4wMijsb0UrE6SQy",
            "RecipientLastName": "Last",
            "RecipientFirstName": "First",
            "1_Q1_1": "Not sure ",
            "1_Q2": "Write a comment,I want to talk about this sign in NZSL - contact me",
            "1_Q2_5_TEXT": "Test Comment",
            "2_Q1_1": "No",
            "2_Q2": "",
            "2_Q2_5_TEXT": "comment",
            "3_Q1_1": "not sure ",
            "3_Q2": "",
            "3_Q2_5_TEXT": ""
        },
    ]

    def test_import_view_post_with_no_permission(self):
        """Test that you get 302 Found or 403 Forbidden if you try without csv import permission."""
        response = self.client_noperm.post(reverse('dictionary:import_qualtrics_csv'))
        # Make sure user does not have change_gloss permission.
        self.assertFalse(response.wsgi_request.user.has_perm('dictionary.import_csv'))
        # Should return 302 Found, or 403 Forbidden
        self.assertIn(response.status_code, [403, 302])

    def test_import_view_post_nologin(self):
        """Testing POST with anonymous user."""
        response = self.client_nologin.post(reverse('dictionary:import_qualtrics_csv'))
        # Should return 302 Found, or 403 Forbidden
        self.assertIn(response.status_code, [302, 403])

    def test_import_view_no_post_method(self):
        """Test that using GET re-renders import view"""
        response = self.client.get(reverse('dictionary:import_qualtrics_csv'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.request["PATH_INFO"],
            reverse('dictionary:import_qualtrics_csv')
        )

    def test_import_view_successful_file_upload(self):
        """Test a csv file can successfully be read by the Qualtrics csv import view"""
        file_name = "test.csv"
        csv_content = self._csv_content
        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(self._csv_headers)
            for response in csv_content:
                writer.writerow(response.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )
        expected_validation_records = csv_content[2:5]

        response = self.client.post(
            reverse('dictionary:import_qualtrics_csv'),
            {"file": file},
            format="multipart"
        )
        self.assertEqual(response.status_code, 200)
        session = self.client.session
        self.assertListEqual(expected_validation_records, session["validation_records"])
        self.assertListEqual(["1", "2", "3"], session["question_numbers"])
        self.assertDictEqual({"1": 1, "2": 2, "3": 3}, session["question_glossvideo_map"])

    def test_confirmation_view_confirm_gloss_creation(self):
        """
        Test that the confirm Qualtrics import csv view can successfully create validation records
        for a gloss.
        """
        csv_content = self._csv_content
        s = self.client.session
        s.update({
            "validation_records": csv_content[2:5],
            "question_numbers": ["1", "2", "3"],
            "question_glossvideo_map": {"1": self.glossvideo_1.pk, "2": self.glossvideo_2.pk,
                                        "3": 222}
        })
        s.save()

        check_results_tag = Tag.objects.get(name=settings.TAG_VALIDATION_CHECK_RESULTS)
        ready_for_validation_tag = Tag.objects.get(name=settings.TAG_READY_FOR_VALIDATION)

        response = self.client.post(
            reverse("dictionary:confirm_import_qualtrics_csv"),
            {"confirm": True}
        )
        self.assertEqual(response.status_code, 200)

        self.assertDictEqual(response.context["missing_gloss_question_pairs"], {"3": 222})
        self.assertEqual(response.context["validation_record_count"], 6)
        self.assertEqual(response.context["responses_count"], 3)
        self.assertEqual(response.context["gloss_count"], 2)

        # check the details of the validation records
        validation_qs_gloss_1 = ValidationRecord.objects.filter(gloss=self.gloss_1)
        self.assertTrue(validation_qs_gloss_1.count(), 3)
        self.assertTrue(validation_qs_gloss_1.filter(
            response_id="R_4PuIGsoEF7g76aE",
            respondent_last_name="Doe",
            respondent_first_name="John",
            sign_seen=ValidationRecord.SignSeenChoices.YES.value,
            comment="",
        ).exists())
        self.assertTrue(validation_qs_gloss_1.filter(
            response_id="R_4nejxM9PFHp9JBL",
            respondent_last_name="Doe",
            respondent_first_name="Jane",
            sign_seen=ValidationRecord.SignSeenChoices.NO.value,
            comment="Test Comment",
        ).exists())
        self.assertTrue(validation_qs_gloss_1.filter(
            response_id="R_4wMijsb0UrE6SQy",
            respondent_last_name="Last",
            respondent_first_name="First",
            sign_seen=ValidationRecord.SignSeenChoices.NOT_SURE.value,
            comment="Test Comment"
        ).exists())

        validation_qs_gloss_2 = ValidationRecord.objects.filter(gloss=self.gloss_2)
        self.assertTrue(validation_qs_gloss_2.count(), 3)
        self.assertTrue(validation_qs_gloss_2.filter(
            response_id="R_4PuIGsoEF7g76aE",
            respondent_last_name="Doe",
            respondent_first_name="John",
            sign_seen=ValidationRecord.SignSeenChoices.NO.value,
            comment="comment",
        ).exists())
        self.assertTrue(validation_qs_gloss_2.filter(
            response_id="R_4nejxM9PFHp9JBL",
            respondent_last_name="Doe",
            respondent_first_name="Jane",
            sign_seen=ValidationRecord.SignSeenChoices.NO.value,
            comment="comment",
        ).exists())
        self.assertTrue(validation_qs_gloss_2.filter(
            response_id="R_4wMijsb0UrE6SQy",
            respondent_last_name="Last",
            respondent_first_name="First",
            sign_seen=ValidationRecord.SignSeenChoices.NO.value,
            comment="comment",
        ).exists())

        check_results_tagged_glosses = TaggedItem.objects.get_by_model(Gloss, [check_results_tag])
        self.assertIn(self.gloss_1, check_results_tagged_glosses)
        self.assertIn(self.gloss_2, check_results_tagged_glosses)
        ready_for_validation_tagged_glosses = TaggedItem.objects.get_by_model(
            Gloss, [ready_for_validation_tag]
        )
        self.assertEqual(ready_for_validation_tagged_glosses.count(), 0)

        # re-upload csv file to test duplicate responses are ignored
        s.update({
            "validation_records": [csv_content[5]],
            "question_numbers": ["1", "2", "3"],
            "question_glossvideo_map": {"1": self.glossvideo_1.pk, "2": self.glossvideo_2.pk,
                                        "3": 222}
        })
        s.save()

        response = self.client.post(
            reverse("dictionary:confirm_import_qualtrics_csv"),
            {"confirm": True}
        )
        self.assertEqual(response.status_code, 200)
        new_validation_qs_gloss_1 = ValidationRecord.objects.filter(gloss=self.gloss_1)
        self.assertTrue(new_validation_qs_gloss_1.count(), 3)
        self.assertListEqual(list(validation_qs_gloss_1), list(new_validation_qs_gloss_1))

        new_validation_qs_gloss_2 = ValidationRecord.objects.filter(gloss=self.gloss_2)
        self.assertTrue(new_validation_qs_gloss_2.count(), 3)
        self.assertListEqual(list(validation_qs_gloss_2), list(new_validation_qs_gloss_2))

    def test_confirmation_view_cancel_gloss_creation(self):
        csv_content = self._csv_content
        s = self.client.session
        s.update({
            "validation_records": csv_content[2:5],
            "question_numbers": ["1"],
            "question_gloss_map": {"1": 1}
        })
        s.save()

        response = self.client.post(
            reverse("dictionary:confirm_import_qualtrics_csv"),
            {"cancel": True}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse("dictionary:import_qualtrics_csv"))
        new_session = self.client.session
        self.assertNotIn("validation_records", new_session.keys())
        self.assertNotIn("question_numbers", new_session.keys())
        self.assertNotIn("question_gloss_map", new_session.keys())

    def test_confirmation_view_no_post_method(self):
        """Test that using GET redirects to import view"""
        response = self.client.get(reverse('dictionary:confirm_import_qualtrics_csv'))
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse("dictionary:import_qualtrics_csv"))


class ManualValidationCSVImportTestCase(TestCase):
    def setUp(self):
        # Create user and add permissions
        self.user = User.objects.create_user(username="test", email=None, password="test")
        csv_permission = Permission.objects.get(codename='import_csv')
        self.user.user_permissions.add(csv_permission)

        # Create client with change_gloss permission.
        self.client = Client()
        self.client.force_login(self.user)

        # Create user with no permissions
        self.user_noperm = User.objects.create_user(username="noperm", email=None,
                                                    password="noperm")
        self.client_noperm = Client()
        self.client_noperm.force_login(self.user_noperm)

        # Create client not logged in
        self.client_nologin = Client()

        # Create a gloss
        # Migrations have id=1 already
        self.signlanguage = SignLanguage.objects.create(pk=2, name="testsignlanguage",
                                                        language_code_3char="tst")
        self.dataset = Dataset.objects.create(name="testdataset", signlanguage=self.signlanguage)
        self.gloss_1 = Gloss.objects.create(idgloss="testgloss:1", dataset=self.dataset)
        self.gloss_2 = Gloss.objects.create(idgloss="testgloss:2", dataset=self.dataset)

        # Assign view permissions to dataset for user
        assign_perm('view_dataset', self.user, self.dataset)

    # Unimportant columns are excluded from csv
    _csv_headers = [
        "group",
        "idgloss",
        "yes",
        "no",
        "abstain or not sure",
        "comments"
    ]
    _csv_content = [
        {
            "group": "Test",
            "idgloss": "testgloss:1",
            "yes": "1",
            "no": "",
            "abstain or not sure": "",
            "comments": "comment"
        },
        {
            "group": "Test",
            "idgloss": "testgloss:2",
            "yes": "",
            "no": "0",
            "abstain or not sure": "1",
            "comments": ""
        },
        # gloss does not exist in test scenario
        {
            "group": "Test",
            "idgloss": "testgloss:222",
            "yes": "1",
            "no": "",
            "abstain or not sure": "",
            "comments": "comment"
        },
    ]

    def test_import_view_post_with_no_permission(self):
        """Test that you get 302 Found or 403 Forbidden if you try without csv import permission."""
        response = self.client_noperm.post(reverse('dictionary:import_manual_validation_csv'))
        # Make sure user does not have change_gloss permission.
        self.assertFalse(response.wsgi_request.user.has_perm('dictionary.import_csv'))
        # Should return 302 Found, or 403 Forbidden
        self.assertIn(response.status_code, [403, 302])

    def test_import_view_post_nologin(self):
        """Testing POST with anonymous user."""
        response = self.client_nologin.post(reverse('dictionary:import_manual_validation_csv'))
        # Should return 302 Found, or 403 Forbidden
        self.assertIn(response.status_code, [302, 403])

    def test_import_view_no_post_method(self):
        """Test that using GET re-renders import view"""
        response = self.client.get(reverse('dictionary:import_manual_validation_csv'))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.request["PATH_INFO"],
            reverse('dictionary:import_manual_validation_csv')
        )

    def test_import_view_missing_required_header(self):
        """Test a missing column re-renders import view"""
        file_name = "test.csv"
        csv_content = copy.deepcopy(self._csv_content)
        csv_headers = copy.deepcopy(self._csv_headers)
        csv_headers.pop()

        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(csv_headers)
            for response in csv_content:
                del response["comments"]
                writer.writerow(response.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )

        response = self.client.post(
            reverse('dictionary:import_manual_validation_csv'),
            {"file": file},
            format="multipart"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.request["PATH_INFO"],
            reverse('dictionary:import_manual_validation_csv')
        )

    def test_non_integer_row_value_raises_validation_error(self):
        """Test a non_compliant row value raises a ValidationError"""
        file_name = "test.csv"
        csv_content = copy.deepcopy(self._csv_content)[0]
        csv_headers = copy.deepcopy(self._csv_headers)

        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(csv_headers)
            csv_content["no"] = "not integer"
            writer.writerow(csv_content.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )

        response = self.client.post(
            reverse('dictionary:import_manual_validation_csv'),
            {"file": file},
            format="multipart"
        )
        self.assertRaises(ValidationError)
        self.assertEqual(response.status_code, 200)

    def test_import_view_successful_file_upload(self):
        """Test a csv file can successfully be read by manual validation csv import view"""
        file_name = "test.csv"
        csv_content = self._csv_content

        with open(file_name, "w") as file:
            writer = csv.writer(file)
            writer.writerow(self._csv_headers)
            for response in csv_content:
                writer.writerow(response.values())
        data = open(file_name, "rb")
        file = SimpleUploadedFile(
            content=data.read(), name=data.name, content_type="content/multipart"
        )

        response = self.client.post(
            reverse('dictionary:import_manual_validation_csv'),
            {"file": file},
            format="multipart"
        )

        self.assertEqual(response.status_code, 200)
        session = self.client.session
        self.assertListEqual(sorted(session["glosses"]), ["1", "2", "222"])
        self.assertDictEqual({"Test": csv_content}, session["group_row_map"])

    def test_confirmation_view_confirm_manual_validation_aggregation_creation(self):
        """
        Test that the confirm manual validation import csv view can successfully create validation records
        for a gloss.
        """
        csv_content = copy.deepcopy(self._csv_content)
        csv_content[0]["idgloss"] = f"testgloss:{self.gloss_1.pk}"
        csv_content[1]["idgloss"] = f"testgloss:{self.gloss_2.pk}"

        s = self.client.session
        s.update({
            "glosses": [str(self.gloss_1.pk), str(self.gloss_2.pk), "222"],
            "group_row_map": {"Test": csv_content}
        })
        s.save()

        response = self.client.post(
            reverse("dictionary:confirm_import_manual_validation_csv"),
            {"confirm": True}
        )
        self.assertEqual(response.status_code, 200)

        self.assertListEqual(response.context["missing_glosses"], [("Test", "testgloss:222")])

        # check the details of the validation aggregations
        manual_validations_gloss_1_qs = ManualValidationAggregation.objects.filter(
            gloss=self.gloss_1)
        self.assertTrue(manual_validations_gloss_1_qs.count(), 1)
        self.assertTrue(manual_validations_gloss_1_qs.filter(
            sign_seen_yes=1,
            sign_seen_no=0,
            sign_seen_not_sure=0,
            group="Test",
            comments="comment",
        ).exists())

        manual_validations_gloss_2_qs = ManualValidationAggregation.objects.filter(
            gloss=self.gloss_2)
        self.assertTrue(manual_validations_gloss_2_qs.count(), 1)
        self.assertTrue(manual_validations_gloss_2_qs.filter(
            sign_seen_yes=0,
            sign_seen_no=0,
            sign_seen_not_sure=1,
            group="Test",
            comments="",
        ).exists())

    def test_confirmation_view_cancel_manual_validation_aggregation_creation(self):
        csv_content = self._csv_content
        s = self.client.session
        s.update({
            "glosses": [str(self.gloss_1.pk), str(self.gloss_2.pk), "222"],
            "group_row_map": {
                "Test": csv_content}
        })
        s.save()

        response = self.client.post(
            reverse("dictionary:confirm_import_manual_validation_csv"),
            {"cancel": True}
        )
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse("dictionary:import_manual_validation_csv"))
        new_session = self.client.session
        self.assertNotIn("glosses", new_session.keys())
        self.assertNotIn("group_row_map", new_session.keys())

    def test_confirmation_view_no_post_method(self):
        """Test that using GET redirects to import view"""
        response = self.client.get(reverse('dictionary:confirm_import_manual_validation_csv'))
        self.assertEqual(response.status_code, 302)
        self.assertRedirects(response, reverse("dictionary:import_manual_validation_csv"))
