# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import csv
import io

from django.contrib.auth.models import AnonymousUser, Permission, User
from django.test import Client, TestCase
from django.urls import reverse


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
        permission = Permission.objects.get(codename="export_csv")
        self.user.user_permissions.add(permission)
        self.user.save()

        response = self.client.get(
            reverse("dictionary:admin_gloss_list"),
            {"format": "CSV-ready-for-validation"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "text/csv; charset=utf-8")
        self.assertEqual(
            response.headers["Content-Disposition"],
            'attachment; filename="ready-for-validation-export.csv"',
        )

        content = response.content.decode('utf-8')
        cvs_reader = csv.reader(io.StringIO(content))
        # this file is empty, so only the headers are present
        headers = list(cvs_reader)[0]
        self.assertEqual(['idgloss', 'gloss_main', 'video_url'], headers)

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