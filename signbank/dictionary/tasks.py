import boto3
import os
from tempfile import TemporaryDirectory
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
    video_type: str
    version: int


def move_glossvideo_to_valid_filepath(glossvideo):
    """
    Mimics the rename_file method on GlossVideo without changing the assigned filename to
    include the video_type (would cause issues with uniqueness), but still adds the pk-folder into
    the name.

    rename_file gives the file a new name of the format {glosspk}-{idgloss}_{videotype}_{pk}{ext}.

    get_valid_name method on the GlossVideoStorage (non AWS S3 FileStorage) class splits the name between gloss_pk and idgloss,
    then joins it back together as {gloss_pk}/{glosspk}-{idgloss}_{videotype}_{pk}{ext}, and
    then joins that with glossvideo. So the required end result is
    glossvideo/{gloss_pk}/{glosspk}-{idgloss}_{videotype}_{pk}{ext}

    In the GlossVideoStorage (non AWS S3 FileStorage) case we need to give get_valid_name our filename, which at this point looks like
    /app/media/temp_dir/{glosspk}-{idgloss}_{unique_name}_{pk}{ext}, so we split at / and give
    get_valid_name only the last bit.

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

    main_video_type = FieldChoice.objects.filter(field="video_type", english_name="main").first()
    finalexample_1_video_type = FieldChoice.objects.filter(
        field="video_type",
        english_name="finalexample1"
    ).first()
    finalexample_2_video_type = FieldChoice.objects.filter(
        field="video_type",
        english_name="finalexample2"
    ).first()
    video_type_map = {
        "main": main_video_type,
        "finalexample1": finalexample_1_video_type,
        "finalexample2": finalexample_2_video_type
    }
    videos_to_create = []

    temp_dir = TemporaryDirectory(dir=settings.MEDIA_ROOT)

    s3 = boto3.client("s3")
    s3_storage_used = settings.GLOSS_VIDEO_FILE_STORAGE == "storages.backends.s3boto3.S3Boto3Storage"

    for video in video_details:
        retrieval_url = f"{settings.NZSL_SHARE_HOSTNAME}{video['url']}"

        if s3_storage_used:
            file, _ = urlretrieve(
                retrieval_url,
                video["file_name"]
            )
            s3.upload_file(
                file, settings.AWS_STORAGE_BUCKET_NAME, video["file_name"]
            )
        else:
            file_name = f"{temp_dir.name}/{video['file_name']}"
            file, _ = urlretrieve(
                retrieval_url,
                file_name
            )

        gloss = Gloss.objects.get(pk=video["gloss_pk"])

        gloss_video = GlossVideo(
            gloss=gloss,
            dataset=gloss.dataset,
            videofile=file,
            title=file,
            version=video["version"],
            is_public=False,
            video_type=video_type_map.get(video["video_type"], None)
        )

        if not s3_storage_used:
            gloss_video = move_glossvideo_to_valid_filepath(gloss_video)
        videos_to_create.append(gloss_video)

    GlossVideo.objects.bulk_create(videos_to_create)

    temp_dir.cleanup()
    connection.close()
