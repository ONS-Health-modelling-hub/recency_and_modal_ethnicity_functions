"""
This script is a basic example of how the latest, modal and re-allocation functions can be used.
The script will need to be added to or re-worked for your project before running.

As set out in this example, ensure you use the 'modal' ethnicity function before the 'latest'
ethnicity function (and not the other way around). This is because at the end of the modal
function, it ensures every row in the original dataset remains, ready to be fed into
the latest ethnicity function. The latest function filters to 1 ethnicity per person
where there are no duplicates.
This means if you use latest first, only part of the dataset will be fed into modal.

Please note:
After running these functions, GP data will still contain some people with multiple rows if
they have the same ethnicity repeated on the same day.
This is to give flexibility in how you wish to de-duplicate after the functions. You could
simply de-duplicate, or give a hierarchy to the suppliers.

Ensure you have:
1. Installed and loaded libraries (see below)
2. Set up your spark session
3. Read in functions from functions.py
4. Updated the config section for your data and project needs (see below)
5. Included code for any data pre-processing
6. After recency/modal functions add in any additional processing needed, including
    de-duplication for GP
7. saved config, list of counts and final dataset at the end

The following example is for reallocating unknown, not stated and any other ethnicity in
GP patient data.

"""
import sys
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

"""
These are the dictionaries used for the different reallocation rules.
not_valid_list: These are ethnic categories at the bottom of the hierarchy and will always be re-allocated.
other_list:     This is a list of ethnic categories you want to re-allocate, but give priority over
                not_valid_list.

These can be edited to suit what you need.
"""

no_reallocation_dict = {"reallocation_type": "No_reallocation",
                        "not_valid_list": [],
                        "other_list": [],
                        "name_of_file" : "Not_reallocated"}

unknown_only_dict = {"reallocation_type": "Reallocation",
                     "not_valid_list": ["99", "X", "-1", "-3"],
                     "other_list": [],
                     "name_of_file" : "Unknown_only"}

unknown_and_any_other_dict = {"reallocation_type": "Reallocation",
                              "not_valid_list": ["99", "X", "-1", "-3"],
                              "other_list": ["S"],
                              "name_of_file" : "Unknown_and_any_other_reallocated"}


unknown_and_not_stated_dict = {"reallocation_type": "Reallocation",
                               "not_valid_list": ["99", "X", "-1", "-3", "Z"],
                               "other_list": [],
                               "name_of_file" : "Unknown_not_stated_only"}

unknown_not_stated_and_any_other_dict = {"reallocation_type": "Reallocation",
                                         "not_valid_list": ["99", "X", "-1", "-3", "Z"],
                                         "other_list": ["S"],
                                         "name_of_file" : "Unknown_not_stated_and_any_other_reallocated"}

all_reallocation_dict = {"reallocation_type": "Reallocation",
                         "not_valid_list": ["99", "X", "-1", "-3", "Z"],
                         "other_list": ["S", "G", "L", "P", "C"],
                         "name_of_file" : "All_reallocation"}

##############################################################################
## Config
##############################################################################
gp_table_name = "GP data directory here" #String, path to where your GP data is saved
data_source = "GP" #String, which data source you are using, options are: "GP", "HES", "IAPT"
date_col_choice = "date_of_journal_item" #String, date column name in your dataset
nhs_col_choice = "nhs_number" #String, nhs number column name in your dataset
ethnicity_col_choice = "ethnic" #String, ethnicity column name in your dataset

reallocation_type = unknown_not_stated_and_any_other_dict["reallocation_type"]
not_valid_list = unknown_not_stated_and_any_other_dict["not_valid_list"]
other_list = unknown_not_stated_and_any_other_dict["other_list"]
name_of_file = unknown_not_stated_and_any_other_dict["name_of_file"]


##############################################################################
## Set up extra large spark session
##############################################################################
spark = (SparkSession.builder.appName("xl-session")
         .config("spark.executor.memory", "20g")
         .config("spark.yarn.executor.memoryOverhead", "2g")
         .config("spark.executor.cores", 5)
         .config("spark.dynamicAllocation.enabled", "true")
         .config("spark.dynamicAllocation.maxExecutors", 12)
         .config("spark.sql.shuffle.partitions", 240)
         .config("spark.shuffle.service.enabled", "true")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.codegen.wholeStage", "false")
         .enableHiveSupport()
         .getOrCreate())

##############################################################################
## import functions
##############################################################################

from functions import *

##############################################################################
## Running script
##############################################################################

list_of_counts = pd.DataFrame(data={"Datasource": [data_source]*3,
                                    "Reallocation": [name_of_file]*3,
                                    "Name":["Not valid list", "Other list", "Name of run"],
                                    "Counts":[str(not_valid_list), str(other_list), name_of_file],
                                    "Comments": ["", "", ""]})

dataframe = spark.read.table(gp_table_name)

# Add any data pre-processing needed here, including removing any nulls in ethnicity column.

if reallocation_type == "Reallocation":
    dataframe_with_ethnicity, list_of_counts = get_modal_ethnicity(
        dataframe=dataframe,
        nhs_col=nhs_col_choice,
        ethnicity_col=ethnicity_col_choice,
        dataset=data_source,
        list_of_counts=list_of_counts,
        not_valid=not_valid_list,
        other=other_list)

    dataframe_with_ethnicity, list_of_counts = get_latest_ethnicity(
        dataframe=dataframe_with_ethnicity,
        date_col=date_col_choice,
        nhs_col=nhs_col_choice,
        ethnicity_col=ethnicity_col_choice,
        dataset=data_source,
        list_of_counts=list_of_counts,
        not_valid=not_valid_list,
        other=other_list)

if reallocation_type == "No_reallocation":
    dataframe_with_ethnicity = get_modal_ethnicity_no_reallocation(
        dataframe=dataframe,
        nhs_col=nhs_col_choice,
        ethnicity_col=ethnicity_col_choice,
        dataset=data_source)

    dataframe_with_ethnicity = get_latest_ethnicity_no_reallocation(
        dataframe=dataframe_with_ethnicity,
        date_col=date_col_choice,
        nhs_col=nhs_col_choice,
        ethnicity_col=ethnicity_col_choice,
        dataset=data_source)

# Add any processing after functions, including de-duplication for GP
# Save outputs
