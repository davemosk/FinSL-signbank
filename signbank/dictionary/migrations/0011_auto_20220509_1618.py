# Generated by Django 2.2.11 on 2022-05-09 04:18

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('dictionary', '0010_auto_20220215_1531'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='dataset',
            name='copyright_fi',
        ),
        migrations.RemoveField(
            model_name='dataset',
            name='description_fi',
        ),
        migrations.RemoveField(
            model_name='dataset',
            name='public_name_fi',
        ),
        migrations.RemoveField(
            model_name='language',
            name='name_fi',
        ),
        migrations.RemoveField(
            model_name='signlanguage',
            name='name_fi',
        ),
    ]
