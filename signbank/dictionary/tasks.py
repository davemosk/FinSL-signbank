from urllib.request import urlretrieve

from background_task import background
from django.conf import settings

from .models import FieldChoice, Gloss
from ..video.models import GlossVideo


def move_glossvideo_to_valid_filepath(glossvideo):
    """
    Mimics the rename_file method on GlossVideo without changing the assigned filename.

    This step is necessary because we create the videos in bulk, and usually the filename and path
    are updated in the save() step.
    We only want the path updates, changing the filename would lead to loss of unique file names
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


# background task schedule kicks task off 30 seconds after call
@background(schedule=30)
def retrieve_videos_for_glosses(video_details, gloss_pks):
    """
    Takes a list of dictionaries of video details.
    The dictionary should contain the following keys:
    - url: url for the video file to be retrieved without the hostname
    - file_name: particular filename that has been created for video
    - gloss_pk: the pk of the gloss for which the GlossVideo is going to be created
    - title
    - version

    Also takes a list of gloss_pks to retrieve a queryset of glosses for which videos will be
    created.
    """
    glosses = Gloss.objects.filter(pk__in=gloss_pks).select_related("dataset")
    gloss_dict = {gloss.pk: gloss for gloss in glosses}
    video_type = FieldChoice.objects.get(field="video_type", english_name="validation")
    videos_to_create = []

    for video in video_details:
        file, _ = urlretrieve(
            f"{settings.NZSL_SHARE_HOSTNAME}{video['url']}",
            video["file_name"]
        )
        gloss = gloss_dict[video['gloss_pk']]
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
