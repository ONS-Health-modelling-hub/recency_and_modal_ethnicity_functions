"""
5 functions for modal, recency and re-allocation methods.
Please see doc strings at the top of each function for in depth description.

1. initial_clean: re-allocation code
2. get_modal_ethnicity: modal ethnicity with re-allocation code
3. get_latest_ethnicity: latest ethnicity with re-allocation code
4. get_modal_ethnicity_no_reallocation: basic modal ethnicity function (no re-allocation)
5. get_latest_ethnicity_no_reallocation: basic latest ethnicity function (no re-allocation)
"""
import sys
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def initial_clean(dataframe,
                  ethnicity_col,
                  nhs_col,
                  other_list,
                  not_valid_list):
    """
    This function is used within get_modal_ethnicity() and get_latest_ethnicity().
    It pre-processes the dataframe based on the ethnic groups within other_list and not_valid_list.
    It is the re-allocation code.

    1. The code creates a hierarchy based on chosen other_list and not_valid_list, where:
        a. if other_list is empty, all ethnic groups in not_valid_list will be given 0, and
          all other ethnic groups 1
        b. if other_list is not empty, all ethnic groups in not_valid_list will be given 0,
          all ethnic groups in other_list is given 1, and all other ethnic groups given 2.
    2. Removes rows that are lower than the maximum hierarchy number for each person.
    
    Only rows that contain the maximum ethnicity hierarchy number will be kept for
    each person. Rows with lower numbers will be removed. This means if someone only
    contains unknown ethnic category in their history, all of their rows will be given 0, and therefore
    that persons maximum ethnicity hierarchy number is 0.

    Parameters
    -----------
    dataframe (PySpark): dataframe
    ethnicity_col (string): ethnicity column name
    nhs_col (string): nhs number column name
    other_list (list of strings): list of 'Other' ethnic groups to reallocate. Can be left blank.
    not_valid_list (list of strings): list of 'not valid' ethnic groups to reallocate.

    Returns
    --------
    dataframe (PySpark): dataframe

    """

    #------------------------------------------ 1. create hierarchy ---------------------------------------------------------

    #If other_list is not empty, then re-allocate ethnic groups in other_list
    if other_list:
        other = True
    else:
        other = False

    if other == True:
        dataframe = dataframe.withColumn(
            "is_other",
            F.when((F.col(ethnicity_col).isin(other_list)),
                   F.lit(1)).otherwise(0))

        #Create hierarchy for valid (2), other (1), or not_valid(0)

        dataframe = dataframe.withColumn(
            "ethnicity_hierarchy",
            F.when((F.col("is_other") == "1"),
                   F.lit(1)) #If rows contain 'other' then set to 1
            .otherwise(2)) #Otherwise make all other rows 2

        dataframe = dataframe.withColumn(
            "ethnicity_hierarchy",
            F.when((F.col(ethnicity_col).isin(not_valid_list)),
                   F.lit(0))
            .otherwise(F.col("ethnicity_hierarchy")))

    else:
        dataframe = dataframe.withColumn(
            "ethnicity_hierarchy",
            F.when((F.col(ethnicity_col).isin(not_valid_list)),
                   F.lit(0))
            .otherwise(1))

    #------------------------------------------- 2. Remove rows that are lower than the max ethnicity hierarchy value for each person ---------------------------------

    # For each person, we will keep only the rows with the highest ethnicity
    # hierarchy value, removing anything else.
    # This means that if a person's data contained 3 ethnic groups in all re-allocation methods:
    # White Irish, other and unknown then White Irish would be chosen as their ethnicity, regardless of frequency or date.
    dataframe = dataframe.withColumn("maxB", F.max("ethnicity_hierarchy").over(Window.partitionBy(nhs_col))).where(F.col("ethnicity_hierarchy") == F.col("maxB")).drop("maxB")

    #Count number of unique ethnic groups per person
    dataframe = dataframe.withColumn(
        "distinct_count_of_hierarchy",
        F.approx_count_distinct("ethnicity_hierarchy")\
            .over(Window.partitionBy(nhs_col)))

    dataframe = dataframe.drop("distinct_count_of_hierarchy")

    return dataframe



def get_modal_ethnicity(dataframe,
                        nhs_col,
                        ethnicity_col,
                        dataset,
                        list_of_counts,
                        not_valid,
                        other):
    """
    What it does:
    This method gets the most frequent ethnicity for each person, after re-allocation.
    Where there are conflicts in most frequent ethnicity it will be set to 'unresolved'.

    Steps taken:
    1. The dataframe undergoes initial_clean function which re-allocates the ethnicity
        codes depending on what reallocation type has been chosen.
    2. Takes output from initial_clean and counts number of unique ethnic groups per person.
    3. For people with just 1 unique ethnicity all duplicated rows are dropped. This
        ethnicity is taken as their modal ethnicity. This dataset is saved as dataframe_one_only.
    4. For people with more than 1 unique ethnicity, frequency of each unique ethnicity is
        calculated. The dataset is then filtered for the most frequent ethnic groups for each person.
    5. The dataset is then checked for people who still have more than 1 unique ethnicity after
        filtering for most frequent. This means they have conflicting modal ethnicity. For these
       people set their modal ethnicity to 'unresolved'.
    6. Drop all duplicated rows. This dataset is saved as dataframe.
    7. dataframe and dataframe_one_only are appended to each other.
    8. This new modal_ethnicity_column is then left joined back onto the original dataset
        so the full dataset can be fed back into the latest function.

    Parameters
    -----------
    dataframe (PySpark): dataframe
    nhs_col (String): nhs number column name
    ethnicity_col (String): ethnicity column name
    dataset (String): data source e.g. "GP", "HES", "IAPT"
    list_of_counts (list): list of counts
    not_valid (list of strings): list of "not valid" ethnic groups to reallocate.
    other (list of strings): list of "Other" ethnic groups to reallocate. Can be left blank.


    Returns
    --------
    dataframe (PySpark): dataframe
    list_of_counts: list

    """
    person_window = Window.partitionBy(nhs_col)
    person_ethnicity_window = Window.partitionBy(nhs_col,
                                                 ethnicity_col)

    dataframe_full = dataframe

    #-------------------------------- 1. Reallocation via initial clean function ------------------------------
    dataframe = initial_clean(dataframe, ethnicity_col, nhs_col, other, not_valid)

    #--------------------------------2. Count number of unique ethnic groups per person--------------------------
    dataframe = dataframe.withColumn(
        "distinct_count_of_ethnicity",
        F.approx_count_distinct(ethnicity_col)\
            .over(Window.partitionBy(nhs_col)))

    #------------------3. Drop records when just 1 unique ethnicity per person -----------------------------------
    # Separate those with just 1 unique ethnicity and reduce to 1 row, keep for later
    dataframe_one_only = dataframe.filter(dataframe.distinct_count_of_ethnicity == 1)
    dataframe_one_only = dataframe_one_only.dropDuplicates([nhs_col])
    dataframe = dataframe.filter(dataframe.distinct_count_of_ethnicity != 1) #Continue with this
    dataframe_one_only = dataframe_one_only.withColumn(
        "modal_ethnicity",
        F.col(ethnicity_col))

    #-------------------------------------- 4, 5, 6. Finding modal ethnicity --------------------------------------
    # How many times does each ethnicity arise for each person?
    dataframe = dataframe.withColumn(
        "count_of_ethnicity",
        F.count(ethnicity_col).over(person_ethnicity_window))

    # Create column containing ethnicity with highest count
    dataframe = dataframe.withColumn(
        "max_count_of_ethnicity",
        F.max("count_of_ethnicity").over(person_window))

    # Which ethnicity has the most occurrences?
    dataframe = dataframe.withColumn(
        "modal_ethnicity",
        F.when(F.col("count_of_ethnicity") == F.col("max_count_of_ethnicity"),
               F.col(ethnicity_col)))

    # Check for multiple modal ethnic groups
    dataframe = dataframe.withColumn(
        "multiple_max_ethnicities",
        F.approx_count_distinct("modal_ethnicity").over(person_window))

    # If more than 1 modal ethnicity then put unresolved
    dataframe = dataframe.withColumn(
        "modal_ethnicity",
        F.when(F.col("multiple_max_ethnicities") > 1, "unresolved")
        .otherwise(F.col("modal_ethnicity")))

    # Fill in the entire modal ethnicity column with this value
    dataframe = dataframe.withColumn(
        "modal_ethnicity",
        F.max(F.col("modal_ethnicity")).over(person_window))

    #Select 1 row per person
    dataframe = dataframe.dropDuplicates([nhs_col, "modal_ethnicity"])

    #---------------------------------------7. Bind dataframe and dataframe_one_only -----------------------------
    dataframe = dataframe.drop("is_other",
                               "count_of_ethnicity",
                               "valid_ethnicity_result",
                               "max_count_of_ethnicity",
                               "max_count_of_ethnicity",
                               "multiple_max_ethnicities")
    dataframe_one_only = dataframe_one_only.drop("is_other")
    dataframe = dataframe.select(sorted(dataframe.columns))
    dataframe_one_only = dataframe_one_only.select(sorted(dataframe_one_only.columns))

    #Dataframe containing 1 ethnicity per person
    dataframe = dataframe.union(dataframe_one_only)

    #---------------------------------------8. left join modal column back onto original dataset -----------------------------
    #As this dataframe is used again in get_latest_ethnicity, we want to get back all the original rows,
    #keeping the 'modal_ethnicity' column
    dataframe = dataframe.withColumnRenamed(nhs_col, "nhs_number2")
    dataframe_full = dataframe_full.withColumnRenamed(nhs_col, "nhs_number")
    dataframe = dataframe_full.join(dataframe.select("nhs_number2", "modal_ethnicity"),
                                    dataframe_full.nhs_number == dataframe.nhs_number2,
                                    how="left").drop("nhs_number2")
    dataframe = dataframe.withColumnRenamed("nhs_number", nhs_col)

    dataframe

    return dataframe, list_of_counts


def get_latest_ethnicity(dataframe,
                         date_col,
                         nhs_col,
                         ethnicity_col,
                         dataset,
                         list_of_counts,
                         not_valid,
                         other):
    """
    What it does:
    This method gets the most recent ethnicity for each person, after re-allocation.
    Where there are conflicts in most recent ethnicity it will be set to 'unresolved'.

    Steps taken:
    1. The dataframe undergoes initial_clean function which re-allocates the ethnicity
        codes depending on what reallocation type has been chosen.
    2. Takes output from initial_clean and counts number of unique ethnic groups per person.
    3. For people with just 1 unique ethnicity all duplicated rows are dropped. This
        ethnicity is taken as their recent ethnicity. This dataset is saved as dataframe_one_only.

    For people with more than unique ethnicity:
    4. If dataset == 'HES': apply hierarchy, where for each person on each unique date,
        apply a hierarchy where ethnicity recorded in HES-APC is taken over HES-AE,
        which is taken over HES-OP. This resolves any conflicts where there are
        different ethnic groups recorded by different HES source on the same day.
    5. Calculate and select records on most recent date
    6. If dataset == 'GP': apply hierarchy, where if there are two ethnic groups across
        snomed and ethnic column, then prioritise ethnicity recorded in snomed column
        This is represented by a flag 'ethnicity_code_present' == 1, where 1 means
        it comes from the snomed column.
    7. If there are still multiple unique ethnic groups recorded on the same day,
        set as recency column as 'unresolved' for those people.
    8. dataframe and dataframe_one_only are appended to each other.
    9. If dataset is not 'GP', then de-duplicate. This is not done at this stage
        for GP as the user may wish to de-duplicate whilst considering GP supplier.

    Parameters
    -----------
    dataframe (PySpark): pyspark dataframe
    date_col (String): date column name
    nhs_col (String): nhs number column name
    ethnicity_col (String): ethnicity column name
    dataset (String): data source e.g. "GP", "HES", "IAPT"
    list_of_counts (list): list of counts
    not_valid (list of strings): list of "not valid" ethnic groups to reallocate.
    other (list of strings): list of "Other" ethnic groups to reallocate. Can be left blank.


    Returns
    --------
    dataframe (PySpark): dataframe
    list_of_counts: list
    """
    #-------------------------------- 1. Reallocation via initial clean function ------------------------------

    dataframe = initial_clean(dataframe, ethnicity_col, nhs_col, other, not_valid)

    #--------------------------------2. Count number of unique ethnic groups per person--------------------------
    #Count number of unique ethnic groups per person
    dataframe = dataframe.withColumn(
        "distinct_count_of_ethnicity",
        F.approx_count_distinct(ethnicity_col)\
            .over(Window.partitionBy(nhs_col)))

    #------------------3. Drop records when just 1 unique ethnicity per person -----------------------------------
    # Separate those with just 1 unique ethnicity and take most recent row, keep for later
    dataframe_one_only = dataframe.filter(dataframe.distinct_count_of_ethnicity == 1)
    dataframe_one_only = dataframe_one_only.withColumn("maxB", F.max(date_col).over(Window.partitionBy(nhs_col))).where(F.col(date_col) == F.col("maxB")).drop("maxB", "distinct_count_of_valid_ethnicity")

    dataframe = dataframe.filter(dataframe.distinct_count_of_ethnicity != 1) #Continue with this
    dataframe_one_only = dataframe_one_only.withColumn(
        "latest_ethnicity",
        F.col(ethnicity_col))

    #-------------------------------------- 4, 5, 6, 7. Finding recent ethnicity --------------------------------------
    #----------------------------- 4. Apply HES hierarchy
    if dataset == "HES":
        dataframe = dataframe.withColumn(
            "ordervar",
            F.when(F.col("table") == "apc", 1).when(F.col("table") == "ae", 2).when(F.col("table") == "op", 3)
            )
        dataframe = dataframe.withColumn("minB", F.min("ordervar").over(Window.partitionBy(nhs_col, date_col))).where(F.col("ordervar") == F.col("minB")).drop("minB")

    #----------------------------- 5. Select most recent date
    dataframe = dataframe.withColumn(
        "first_date",
        F.max(F.col(date_col)).over(Window.partitionBy(nhs_col))).where(F.col(date_col) == F.col("first_date")).drop("first_date")

    #----------------------------- 6. Apply GP hierarchy
    if dataset == "GP":
        #Apply GP hierarchy
        dataframe = dataframe.withColumn(
            "distinct_count_of_ethnicity_code",
            F.approx_count_distinct("ethnicity_code_present")\
                .over(Window.partitionBy(nhs_col)))

        dataframe = dataframe.withColumn("maxB", F.max("ethnicity_code_present").over(Window.partitionBy(nhs_col, date_col))).where(F.col("ethnicity_code_present") == F.col("maxB")).drop("maxB")

        dataframe = dataframe.withColumn(
            "distinct_count_of_ethnicity_code",
            F.approx_count_distinct("ethnicity_code_present")\
                .over(Window.partitionBy(nhs_col)))

        dataframe = dataframe.drop("distinct_count_of_ethnicity_code")

    #----------------------------- 7. Where conflicts still exist, mark as unresolved
    dataframe = dataframe.withColumn(
        "multiple_records_same_day",
        F.approx_count_distinct(
            ethnicity_col
            ).over(Window.partitionBy(nhs_col, date_col)))

    #Now all records with greater than 1 record per day can be put as unresolved.
    dataframe = dataframe.withColumn(
        "latest_ethnicity",
        F.when(F.col("multiple_records_same_day") > 1,
               F.lit("unresolved")).otherwise(F.col(ethnicity_col)))

    #---------------------------8. bind dataframe and dataframe_one_only---------------------------------------
    dataframe = dataframe.drop("multiple_records_same_day", "ethnicity_hierachy", "is_other", "ordervar")
    dataframe_one_only = dataframe_one_only.drop("ethnicity_hierachy", "is_other")
    dataframe = dataframe.select(sorted(dataframe.columns))
    dataframe_one_only = dataframe_one_only.select(sorted(dataframe_one_only.columns))

    dataframe = dataframe.union(dataframe_one_only)

    #---------------------------9. If not GP data, de-duplicate ---------------------------------------
    if dataset != "GP":
        dataframe = dataframe.drop_duplicates([nhs_col, "latest_ethnicity"])

        dataframe
    return dataframe, list_of_counts


def get_modal_ethnicity_no_reallocation(dataframe, nhs_col, ethnicity_col, dataset):
    """
    What it does:
    This method aims to get the most frequent ethnicity for each person.
    Where there are conflicts in most frequent ethnicity it will be set to 'Unresolved'.

    Steps taken:
      1. For each person, frequency of each unique ethnicity is calculated. The dataset
          is then filtered for the most frequent ethnic groups for each person.
      2. The dataset is then checked for people who still have more than 1 unique ethnicity
          after filtering for their most frequent. This means they have conflicted modal ethnicity.
          For these people set their modal ethnicity to 'unresolved'.
      3. Create modal ethnicity column

    Parameters
    -----------
    dataframe (PySpark): dataframe
    nhs_col (String): nhs number column name
    ethnicity_col (String): ethnicity column name
    dataset (String): data source e.g. "GP", "HES", "IAPT"

    Returns
    --------
    dataframe (PySpark): dataframe

    """
    person_window = Window.partitionBy(nhs_col)
    person_ethnicity_window = Window.partitionBy(nhs_col,
                                                 ethnicity_col)

    # --------------------- 1. For each person, frequency of each unique ethnicity is calculated. The dataset is then filtered for its max frequency count.
    dataframe = dataframe.withColumn(
        "count_of_ethnicity",
        F.count(ethnicity_col).over(person_ethnicity_window))

    dataframe = dataframe.withColumn(
        "max_count_of_ethnicity",
        F.max("count_of_ethnicity").over(person_window))

    # Which ethnicity has the most occurrences?
    dataframe = dataframe.withColumn(
        "modal_ethnicity",
        F.when(F.col("count_of_ethnicity") == F.col("max_count_of_ethnicity"),
               F.col(ethnicity_col)))

    # ----------------- 2. Where there are conflicts, mark as unresolved --------------------------------------------
    dataframe = dataframe.withColumn(
        "multiple_max_ethnicities",
        F.approx_count_distinct("modal_ethnicity").over(person_window))

    dataframe = dataframe.withColumn(
        "modal_ethnicity",
        F.when(F.col("multiple_max_ethnicities") > 1, "unresolved")
        .otherwise(F.col("modal_ethnicity")))

    # -----------------  3. Create modal ethnicity column  --------------------------------------------
    dataframe = dataframe.withColumn(
        "modal_ethnicity",
        F.max(F.col("modal_ethnicity")).over(person_window))

    return dataframe


def get_latest_ethnicity_no_reallocation(dataframe, date_col, nhs_col, ethnicity_col, dataset):
    """
    What it does:
    This method gets the most recent ethnicity for each person.
    Where there are conflicts in most recent ethnicity it will be set to 'unresolved'.

    Steps taken:
      1. Determine the max date of ethnicity recorded per person
      2. Create latest ethnicity column where if date matches max date, put ethnicity,
          otherwise put as historic
      3. If dataset == 'HES': apply hierarchy, where for each person on each unique date,
          apply a hierarchy where ethnicity recorded in HES-APC is taken over HES-AE,
         which is taken over HES-OP. This resolves any conflicts where there are different
         ethnic groups recorded by different HES source on the same day.
      4. Select records on most recent date for each person
      5. If dataset == 'GP': apply hierarchy, where if there are two ethnic groups across
          snomed and ethnic column, then prioritise ethnicity recorded in snomed column.
          This is represented by a flag 'ethnicity_code_present' == 1, where 1 means it
          comes from the snomed column.
      6. If there are still multiple unique ethnic groups recorded on the same day, set as
          recency column as 'unresolved' for those people.
      7. If dataset is not 'GP', then de-duplicate. This is not done at this stage for GP
          as the user may wish to de-duplicate whilst considering GP supplier.

    Parameters
    -----------
    dataframe (PySpark): dataframe
    date_col (String): date column name
    nhs_col (String): nhs number column name
    ethnicity_col (String): ethnicity column name
    dataset (String): data source e.g. "GP", "HES", "IAPT"

    Returns
    --------
    dataframe (PySpark): dataframe
    """
    person_window = Window.partitionBy(nhs_col)
    person_ethnicity_window = Window.partitionBy(nhs_col,
                                                 ethnicity_col)

    person_date_window = Window.partitionBy(nhs_col)\
        .orderBy(date_col)\
            .rowsBetween(-sys.maxsize, 0)

    # ----------------------------  1. Determine the max date of ethnicity recorded per person --------------------------------------
    dataframe = dataframe.withColumn(
        "max_date_of_ethnicity",
        F.max(date_col).over(person_window))

    # ------------ 2. Create latest ethnicity column where if date matches max date, put ethnicity, otherwise put as historic -------
    # Get most recent version of each persons ethnicity
    dataframe = dataframe.withColumn(
        "latest_ethnicity",
        F.when(F.col(date_col) == F.col("max_date_of_ethnicity"),
               F.col(ethnicity_col))
        .otherwise(F.lit("historic")))

    # -------------------------------------------- 3. Apply HES hierarchy -----------------------------------------------------------
    if dataset == "HES":
        dataframe = dataframe.withColumn(
            "ordervar",
            F.when(F.col("table") == "apc", 1).when(F.col("table") == "ae", 2).when(F.col("table") == "op", 3))

        dataframe = dataframe.withColumn("minB", F.min("ordervar").over(Window.partitionBy(nhs_col, date_col))).where(F.col("ordervar") == F.col("minB")).drop("minB")

    # --------------------------------   4. Select records on most recent date for each person --------------------------------------
    dataframe = dataframe.where('latest_ethnicity != "historic"')

    # -------------------------------------------- 5. Apply GP hierarchy ------------------------------------------------------------
    if dataset == "GP":
        dataframe = dataframe.withColumn("maxB", F.max("ethnicity_code_present").over(Window.partitionBy(nhs_col, date_col))).where(F.col("ethnicity_code_present") == F.col("maxB")).drop("maxB")
    # -------------------------------------------- 6. Set as unresolved for conflicts------------------------------------------------
    dataframe = dataframe.withColumn(
        "multiple_records_same_day?",
        F.approx_count_distinct("latest_ethnicity").over(person_window))

    dataframe = dataframe.withColumn(
        "latest_ethnicity",
        F.when(F.col("multiple_records_same_day?") > 1,
               F.lit("unresolved"))
        .otherwise(F.col("latest_ethnicity"))).drop("multiple_records_same_day?")

    # ---------------------------------------------  7. If dataset is not 'GP', then de-duplicate ------------------------------------
    if dataset != "GP":
        dataframe = dataframe.drop_duplicates([nhs_col, "latest_ethnicity"])

    return dataframe
