# Functions to derive the most recent and most frequent ethnicity using re-allocation in health-related administrative data sources
Code used in ONS publications [_Understanding consistency of ethnicity data recorded in health-related administrative datasets in England: 2011 to 2021_](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthinequalities/articles/understandingconsistencyofethnicitydatarecordedinhealthrelatedadministrativedatasetsinengland2011to2021/2023-01-16) and [_Quality of ethnicity data in health-related administrative data sources, England: November 2023_](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthinequalities/articles/understandingconsistencyofethnicitydatarecordedinhealthrelatedadministrativedatasetsinengland2011to2021/latest) to derive a single ethnicity for each person in General Practice Extraction Service (GPES) Data for Pandemic Planning and Research (GDPPR), Hospital Episode Statistics (HES) and NHS Talking Therapies data, previously called Improving Access to Psychological Therapies (IAPT).

Please note: the code was written before IAPT became NHS Talking Therapies so refers to this dataset as IAPT.

## Getting set up
Please use information in doc strings in main_example.py to get set up with the required libraries and configuration.

## Scripts
### Main_example.py
This is a basic example of how you can set up the functions in your own projects, including what libraries need to be installed. This script also contains the "re-allocation of ethnicity" dictionaries used in our publication.

### Functions.py: 
This contains 5 functions used to derive ethnicity. These include modal, recency and re-allocation code. Please see the doc strings at the top of each function for in depth description.
1. initial_clean: re-allocation code
2. get_modal_ethnicity: modal ethnicity with re-allocation code
3. get_latest_ethnicity: latest ethnicity with re-allocation code
4. get_modal_ethnicity_no_reallocation: basic modal ethnicity function (no re-allocation)
5. get_latest_ethnicity_no_reallocation: basic latest ethnicity function (no re-allocation)

## Methodology
For recency and modal methodology and definitions please see section 3 in [_Understanding consistency of ethnicity data recorded in health-related administrative datasets in England: 2011 to 2021_](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthinequalities/articles/understandingconsistencyofethnicitydatarecordedinhealthrelatedadministrativedatasetsinengland2011to2021/2023-01-16)

For re-allocation methodology and definitions please see section 3 in [_Quality of ethnicity data in health-related administrative data sources, England: November 2023_](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/healthinequalities/articles/understandingconsistencyofethnicitydatarecordedinhealthrelatedadministrativedatasetsinengland2011to2021/latest)

## Terminology
### Person
This refers to a unique NHS number in the health administrative dataset.
### Not valid
This refers to ethnicities at the bottom of the chosen re-allocation hierarchy e.g. Unknown, Not Stated. These will always be re-allocated when there is an alternative ethnicity available.
### Other
This refers to ethnicities in the middle of the chosen re-allocation hierarchy. In our publication, this was the "Any other ethnic group" ethnicity. These ethnic groups are given priority over ethnicities in the "not valid" list but will be re-allocated if there are any ethnicities present which are not in the "other" list or "not valid" list.
### Valid
 This refers to ethnicities in the top of the chosen re-allocation hierarchy and will be chosen over ethnic groups in the not valid or other groups.