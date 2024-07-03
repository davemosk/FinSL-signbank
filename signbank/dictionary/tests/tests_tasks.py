# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import random
import tempfile
from unittest import mock

from django.conf import settings
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.test.utils import override_settings

from signbank.dictionary.models import SignLanguage, Dataset, FieldChoice, Gloss
from signbank.dictionary.tasks import retrieve_videos_for_glosses
from signbank.video.models import GlossVideo


class RetrieveVideoForGloss(TestCase):
    def setUp(self):
        self.signlanguage = SignLanguage.objects.create(pk=2, name="testsignlanguage",
                                                        language_code_3char="tst")
        self.dataset = Dataset.objects.create(name="testdataset", signlanguage=self.signlanguage)

        self.main_vt = FieldChoice.objects.create(
            field="video_type", english_name="main", machine_value=random.randint(0, 99999)
        )
        self.finalexample1_vt = FieldChoice.objects.create(
            field="video_type", english_name="finalexample1",
            machine_value=random.randint(0, 99999)
        )
        self.finalexample2_vt = FieldChoice.objects.create(
            field="video_type", english_name="finalexample2",
            machine_value=random.randint(0, 99999)
        )
        self.gloss = Gloss.objects.create(idgloss="testgloss", dataset=self.dataset)

    @override_settings(MEDIA_ROOT="")
    def test_retrieve_videos_for_glosses(self):

        video_details = [
            {
                "url": "/kiwifruit_1.mp4",
                "file_name": f"{self.gloss.pk}-{self.gloss.idgloss}_finalexample_1.png",
                "gloss_pk": self.gloss.pk,
                "video_type": "finalexample1",
                "version": 0
            }
        ]
        dummy_file = tempfile.NamedTemporaryFile()
        dummy_file.write(b'data \x00\x01')

        with mock.patch("signbank.dictionary.tasks.urlretrieve") as mock_retrieve:
            with mock.patch("signbank.dictionary.tasks.connection.close") as mock_close_connection:
                mock_retrieve.return_value = (dummy_file.name, None)
                mock_close_connection.return_value = None
                retrieve_videos_for_glosses(video_details)
                mock_retrieve.assert_called_once()
                mock_close_connection.assert_called_once()

        videos = GlossVideo.objects.filter(gloss=self.gloss)
        self.assertTrue(videos.exists())
        self.assertEqual(videos.count(), 1)
        video = videos.get()
        self.assertEqual(video.video_type, self.finalexample1_vt)
        self.assertEqual(video.title, dummy_file.name)

