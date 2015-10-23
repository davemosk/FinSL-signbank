from django import forms
from django.utils.translation import ugettext_lazy as _

from models import GlossVideo


class VideoUploadForm(forms.ModelForm):
    """Form for video upload"""

    class Meta:
        model = GlossVideo
        exclude = ()


class VideoUploadForGlossForm(forms.Form):
    """Form for video upload for a particular gloss"""
    # Translators: VideoUploadForGlossForm
    videofile = forms.FileField(label=_("Upload Video"))
    gloss_id = forms.CharField(widget=forms.HiddenInput)
    redirect = forms.CharField(widget=forms.HiddenInput, required=False)
