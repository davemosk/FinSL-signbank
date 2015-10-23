"""Convert a video file to flv"""

import time

from django.core.management.base import BaseCommand

from signbank.video.models import GlossVideo


class Command(BaseCommand):

    help = 'Create JPEG images for all videos'
    args = ''

    def handle(self, *args, **options):

        # just access the poster path for each video
        for vid in GlossVideo.objects.all():
            print vid.poster_path()
            time.sleep(20)
