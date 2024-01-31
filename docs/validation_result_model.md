# Qualitrics exported CSV data and ValidationResult model

Qualitrics gives the ability to export survey data in several formats, 
the one relevant for NZSL Signbank is CSV.

## Example CSV snippet

| StartDate	                                             | EndDate                                              | Status                | IPAddress                | Progress                | Duration (in seconds)   | Finished                | RecordedDate                                              | ResponseId               | RecipientLastName                | RecipientFirstName                | RecipientEmail                | ExternalReference                    | LocationLatitude                | LocationLongitude                | DistributionChannel                | UserLanguage                | Q_BallotBoxStuffing                | 1_Q1_1                                                                                           | 1_Q2                                                                                            | 1_Q2_5_TEXT                                                                                  |
|--------------------------------------------------------|------------------------------------------------------|-----------------------|--------------------------|-------------------------|-------------------------|-------------------------|-----------------------------------------------------------|--------------------------|----------------------------------|-----------------------------------|-------------------------------|--------------------------------------|---------------------------------|----------------------------------|------------------------------------|-----------------------------|------------------------------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Start Date                                             | End Date                                             | Response Type         | IP Address               | Progress                | Duration (in seconds)   | Finished                | Recorded Date                                             | Response ID              | Recipient Last Name              | Recipient First Name              | Recipient Email               | External Data Reference              | Location Latitude               | Location Longitude               | Distribution Channel               | User Language               | Q_BallotBoxStuffing                | 1_Q1 - https://vuw.qualtrics.com/CP/File.php?F=F_78nY3cJ9AWK0XtA - Have seen it or use it myself | Comment - https://vuw.qualtrics.com/CP/File.php?F=F_78nY3cJ9AWK0XtA - Comment - Selected Choice | Comment - https://vuw.qualtrics.com/CP/File.php?F=F_78nY3cJ9AWK0XtA - Write a comment - Text |
| {"ImportId":"startDate","timeZone":"Pacific/Auckland"} | {"ImportId":"endDate","timeZone":"Pacific/Auckland"} | {"ImportId":"status"} | {"ImportId":"ipAddress"} | {"ImportId":"progress"} | {"ImportId":"duration"} | {"ImportId":"finished"} | {"ImportId":"recordedDate","timeZone":"Pacific/Auckland"} | {"ImportId":"_recordId"} | {"ImportId":"recipientLastName"} | {"ImportId":"recipientFirstName"} | {"ImportId":"recipientEmail"} | {"ImportId":"externalDataReference"} | {"ImportId":"locationLatitude"} | {"ImportId":"locationLongitude"} | {"ImportId":"distributionChannel"} | {"ImportId":"userLanguage"} | {"ImportId":"Q_BallotBoxStuffing"} | {"ImportId":"1_QID10_1"}                                                                         | {"ImportId":"1_QID7"}                                                                           | {"ImportId":"1_QID7_5_TEXT"}                                                                 |
| 9/11/2022 17:54                                        | 9/11/2022 18:44                                      | 0                     |                          | 100                     | 2987                    | 1                       | 9/11/2022 18:44                                           | R_UMhF6SuJzvtZE2t        | Doe                              | Joe                               |                               |                                      |                                 |                                  | email                              | EN-GB                       |                                    | Not sure                                                                                         | Write a comment	                                                                                | couldn't see the video                                                                       |
| 10/11/2022 18:23                                       | 11/11/2022 8:57                                      | 0                     |                          | 100                     | 52428                   | 1                       | 11/11/2022 8:57                                           | R_3qqXgb2jvWPRPbR        | Name                             | Random                            |                               |                                      |                                 |                                  | email                              | EN-GB                       |                                    | Not sure                                                                                         | Write a comment,I want to talk about this sign in NZSL - contact me                             | video not showing?                                                                           |
| 10/11/2022 21:53                                       | 11/11/2022 20:49                                     | 0                     |                          | 100                     | 82586                   | 1                       | 11/11/2022 20:49                                          | R_3MrPivulGQ6TJmk        | Someone                          | Else                              |                               |                                      |                                 |                                  | email                              | EN-GB                       |                                    | Yes                                                                                              | Write a comment	                                                                                | I think only one sign for abbreviation - ticked yes but can't see video                      |

## [Qualitrics][qualitrics-data] documentation
### [Format basics][format-basics]

CSV and TSV files come with 3 rows of headers. The first header is the internal Qualtrics ID of 
the field (e.g., EndDate, Q1, Q2, and so on). The second header is the field’s name or text 
(e.g., End Date, How satisfied are you with Qualtrics?). The third header has import IDs. All 3 
of these headers are included because they are needed to upload the data to a survey. Respondent 
data starts on the fourth row of the file.  

### [Respondent information][respondent-information]
The first several columns pertain to information about the respondent. For the purpose of 
importing the data into Signbank most of these columns can be ignored.  
There are a number of different statuses a response can have, supplied in the `Status` column. 
Their values can be:

- `0 / IP Address`: A normal response
- `1 / Survey Preview`: A preview response 
- `2 / Survey Test`: A test response 
- `4 / Imported`: An imported response 
- `8 / Spam`: A possible spam response 
- `9  / Preview Spam`: A possible spam response submitted through the preview link 
- `16 / Offline`: A Qualtrics Offline App response 
- `17 / Offline Preview`: Previews submitted through the Qualtrics Offline App. This feature is deprecated in latest versions of the app

Based on these values it might be worth ignoring anything but 0 and 4

Furthermore, we should take note of the `ResponseId`, `RecipientLastName` and `RecipientFirstName` columns to 
store the name of the validator. Potentially even the `RecipientEmail` column.

For each gloss there are three column headers. In the example provided their names are of the 
format 
1. (`{number}_Q1_1`,`{number}_Q1 - {video_url} - Have seen it or use it myself` , `ImportId {number}_QID10_1`)
2. (`{number}_Q2`, `Comment - {video_url} - Comment - Selected Choice` , `ImportId {number}_QID7`)
3. (`{number}_Q2_5_TEXT`,`Comment - {video_url} - Write a comment - Text`, `ImportId {number}_QID7_5_TEXT`) 

It seems tricky to put the idgloss into the column headers instead of the question number for the 
export, so the idgloss will have to be extracted form the url instead.

The first column corresponds to the question `Have seen it or use it myself` with possible answers 
`Yes`, `No`, `Not sure`. 
In a previous version of the CSV export these columns were filled with values (at a glance) 1,2,4.  
Assumption: 1-Yes, 2-No, 4-Not sure

The second and third columns correspond to a Comment the respondent can leave. The second column  
represents the comment choice, presumably the tick box in the screen shot. 
A newer version of the CSV file shows which tick boxes have been selected, eg. 
`Write a comment,I want to talk about this sign in NZSL - contact me`.
The previous version of the CSV showed the values 5 and 7. This meant that 5 refers to the `write a comment` tickbox, and 7 refers to 
the `I wans to talk to NZSL about this comment` tickbox.  
Column 3 represents the comment itself.

![screenshot][qualitrics-screenshot]

# Proposed model

While there is an option to have a JSON field on a model and store all recorded answers for a 
gloss in that field it might be cleaner to just have a record per respondent per gloss.

```python
from django.db import models


class ValidationRecord(models.Model):
    class SignSeenChoices(models.TextChoices):
        YES = "yes", "Yes"
        NO = "no", "No"
        NOT_SURE = "not_sure", "Not sure"
    gloss = models.ForeignKey(Gloss, related_name="validation_records", on_delete=models.CASCADE)
    sign_seen = models.CharField(
        max_length=50, choices=SignSeenChoices.choices,
        help_text="Result of the survey question 'Have seen it or use it myself'"
    )
    response_id = models.CharField(
        max_length=255, help_text="Identifier of specific survey result in Qualitrics"
    )  # can potentially make this unique
    respondent_first_name = models.CharField(
        max_length=255, default="", help_text="Survey respondents first name"
    )
    respondent_last_name = models.CharField(
        max_length=255, default="", help_text="Survey respondents last name"
    )
    respondent_email = models.EmailField(default="", help_text="Survey respondents email")
    comment = models.TextField(
        default="", help_text="Optional comment the survey respondent can leave about the gloss"
    )
    contact_with_nzsl_requested = models.BooleanField(
        default=False,
        help_text=(
            "Boolean value that indicates if the survey respondent would like to be contacted by "
            "NZSL to discuss the gloss further"
        )
    )
    
```

## ShareValidationAggregation model

As part of NZSL-74 a model has been introduced to capture the amount of people agreeing and 
disagreeing with a gloss from Share. This model is populated during the CSV import of glosses 
from Share. These aggregated results will be displayed along the aggregated results of the 
ValidationRecords imported from Qualitrics, part of NZSL-78.

```python
from django.db import models


class ShareValidationAggregation(models.Model):
    """
    Captures how many people on NZSL Share agree or disagree with a gloss
    """
    gloss = models.ForeignKey(Gloss, related_name="share_validation_aggregations",
                              on_delete=models.CASCADE)
    agrees = models.PositiveIntegerField()
    disagrees = models.PositiveIntegerField()
```

<!-- Links and resources -->
[qualitrics-data]: https://www.qualtrics.com/support/survey-platform/data-and-analysis-module/data/download-data/export-data-overview/#UnderstandingDataSet
[format-basics]: https://www.qualtrics.com/support/survey-platform/data-and-analysis-module/data/download-data/understanding-your-dataset/#Basics
[respondent-information]: https://www.qualtrics.com/support/survey-platform/data-and-analysis-module/data/download-data/understanding-your-dataset/#RespondentInformation
[qualitrics-screenshot]: ./qualitrics-screenshot.png
