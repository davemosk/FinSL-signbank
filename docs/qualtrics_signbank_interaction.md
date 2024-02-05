# Interaction between Qualtrics, NZSL Signbank and NZSL Share

## Qualtrics
Qualtrics is a survey tool used by NZSL to validate new glosses with the public.
It works by displaying a video of a gloss and then asking the survey respondent if they have seen this sign before. 
They also can leave a comment for the sign, or request that NZSL contacts them to discuss the sign further. 

A Qualtrics survey can be prepared via CSV upload. 
The csv file that is uploaded is the ready-for-validation export from Signbank and the format has been determined by NZSL.

The responses to a survey can likewise be downloaded as a CSV file.
Documentation on the format can be found under the [ValidationResult model][validation-result-model]. 
This CSV file is imported into Signbank as the Qualtrics import.

## NZSL Share
NZSL Share is a website that lets users upload proposed glosses for NZSL Signbank. 
Other users can then agree or disagree with a proposed gloss.

Proposed glosses from NZSL Share can be exported to a CSV file at `{NZSL-Share-Host}/admin/exports` as the Signbank export.

## NZSL Signbank
NZSL Signbank is the NZSL's dictionary of official glosses.
New glosses can be created through a CSV import from NZSL Share at `{NZSL-Signbank-Host}/dictionary/advanced/import/csv/nzsl-share/`. 
These glosses are marked as private, so they are not visible to the public using NZSL Signbank.  

New glosses go through an internal validation process before being exported as the ready-for-validation CSV for a public validation survey through Qualtrics here `{NZSL-Signbank-Host}/dictionary/advanced/?format=CSV-ready-for-validation`.  
Results from the survey in Qualtrics can then be imported back into NZSL Signbank at `{NZSL-Signbank-Host}/dictionary/advanced/import/csv/Qualtrics/`. 
The results of the survey, together with users of NZSL Share who agree or disagree with the sign, 
then will lead to a decision about whether a proposed gloss becomes an official gloss.

## Sequence of action
![sequence-diagramm][mermaid-graph]

<!-- Links and resources -->
[mermaid-graph]: ./mermaid-diagram-2024-02-06-131031.png
[validation-result-model]: validation_result_model.md
