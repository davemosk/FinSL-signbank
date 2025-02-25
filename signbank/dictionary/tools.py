import json

from django.conf import settings
from django.utils.translation import gettext as _
from zipfile import ZipFile

from signbank.dictionary.models import Dataset, Gloss


def get_gloss_data(since_timestamp=0, dataset=None):
    """
    This function is copied from Global Signbank.
    It has been adapted to work for NZSL's data structure.
    """
    api_fields_2023 = []
    if not dataset:
        dataset = Dataset.objects.get(name=settings.DEFAULT_DATASET_ACRONYM)
    for language in dataset.translation_languages.all():
        language_field = f"{_('Translations')} {language.name}"
        api_fields_2023.append(language_field)

    api_fields_2023.append("Handedness")
    api_fields_2023.append("Strong Hand")
    api_fields_2023.append("Weak Hand")
    api_fields_2023.append("Location")
    api_fields_2023.append("Semantic Field")
    api_fields_2023.append("Word Classes")
    api_fields_2023.append("Named Entity")
    api_fields_2023.append("Link")
    api_fields_2023.append("Video")

    glosses = Gloss.objects.filter(dataset=dataset)
    gloss_data = {}
    for gloss in glosses:
        if since_timestamp and gloss.updated_at > since_timestamp:
            gloss_data[str(gloss.pk)] = gloss.get_fields_dict(api_fields_2023)
        else:
            gloss_data[str(gloss.pk)] = gloss.get_fields_dict(api_fields_2023)

    return gloss_data


def create_zip_with_json_files(data_per_file, output_path):
    """
    This function is copied from Global Signbank.

    Creates a zip file filled with the output of the functions supplied.
    Data should either be a json string or a list, which will be transformed to json.
    """

    INDENTATION_CHARS = 4

    zip = ZipFile(output_path, 'w')

    for filename, data in data_per_file.items():
        if isinstance(data, list) or isinstance(data, dict):
            try:
                output = json.dumps(data, indent=INDENTATION_CHARS)
            except TypeError:
                print('problem processing json.dumps on ', filename)
                output = ''
            zip.writestr(filename + '.json', output)
    zip.close()
