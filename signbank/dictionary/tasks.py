from typing import TypedDict, List
from urllib.request import urlretrieve

from django.conf import settings
from django.db import connection

from .models import FieldChoice, Gloss
from ..video.models import GlossVideo


class VideoDetail(TypedDict):
    url: str
    file_name: str
    gloss_pk: int
    title: str
    version: int


def move_glossvideo_to_valid_filepath(glossvideo):
    """
    Mimics the rename_file method on GlossVideo without changing the assigned filename to
    include the video_type (would cause issues with uniqueness), but still adds the pk-folder into
    the name.

    This step is necessary because we create the videos in bulk, and usually the filename and path
    are updated in the save() step.
    """
    old_file = glossvideo.videofile
    full_new_path = glossvideo.videofile.storage.get_valid_name(
        glossvideo.videofile.name.split("/")[-1]
    )
    if not glossvideo.videofile.storage.exists(full_new_path):
        # Save the file into the new path.
        saved_file_path = glossvideo.videofile.storage.save(full_new_path, old_file)
        # Set the actual file path to videofile.
        glossvideo.videofile = saved_file_path
    return glossvideo


def retrieve_videos_for_glosses(video_details: List[VideoDetail]):
    """
    Takes a list of dictionaries of video details.

    The dictionary should contain the following keys:
    - url: url for the video file to be retrieved without the hostname
    - file_name: particular filename that has been created for video
    - gloss_pk: the pk of the gloss for which the GlossVideo is going to be created
    - title
    - version
    """
    validation_video_type = FieldChoice.objects.filter(field="video_type",
                                                       english_name="validation").first()
    main_video_type = FieldChoice.objects.filter(field="video_type", english_name="main").first()
    videos_to_create = []

    for video in video_details:
        retrieval_url = f"{settings.NZSL_SHARE_HOSTNAME}{video['url']}"
        file, _ = urlretrieve(
            retrieval_url,
            video["file_name"]
        )
        gloss = Gloss.objects.get(pk=video["gloss_pk"])

        # change video type to main for illustrations
        video_type = validation_video_type
        if video["title"] == "Illustration":
            video_type = main_video_type
            video["title"] = ""

        gloss_video = GlossVideo(
            title=video["title"],
            gloss=gloss,
            dataset=gloss.dataset,
            videofile=file,
            version=video["version"],
            is_public=False,
            video_type=video_type
        )
        videos_to_create.append(
            move_glossvideo_to_valid_filepath(gloss_video)
        )
    GlossVideo.objects.bulk_create(videos_to_create)

    connection.close()
