# Databricks notebook source
# MAGIC %md
# MAGIC This notebook contains all the functions used by the pipeline.
# MAGIC 
# MAGIC The functions are called from the "main" notebook.
# MAGIC 
# MAGIC The first few functions are utilities:
# MAGIC - remove_duplicate_request_response_records
# MAGIC - filter_for_new_records
# MAGIC - dt_merge_unique
# MAGIC - validate_schema
# MAGIC 
# MAGIC And the remaining functions are derivation logic for adding each of the mps diagnostics columns.

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %md
# MAGIC ##DISAMBIGUATE THE MPS_ARCHIVE DATA SET

# COMMAND ----------

def remove_duplicate_request_response_records(df_requests_and_responses:DataFrame, UNMANAGED_DATASETS:List[str]) -> DataFrame:
  '''
  In datasets that are well maintained, records which are not unique on local_id and unique_reference are created due to the same batch of data being submitted more than once. 
  So where there are multiple records with the same local_id and unique_reference, this function disambiguates by keeping the most recent record (on the request date and the response date).
  
  Unmanaged datasets, such as "adhoc", are not well maintained, and there are instances where the same local_id and unique_reference are used for two or more different submissions to MPS, i.e. not the same person.
  In these cases we are unable to reconcile which record in the response file should correspond to each record in the request file.
  In some cases we can tell by inspection due to request and response having the same NHS number or demographic columns. However, to perform this reconciliation reliably would be equivalent to writing a linkage algorithm.
  For this reason, if multiple records in unmanaged datasets have the same local_id and unique_reference, we are not able to tell which pair in the request_response joined table is correct, so this function removes all of the ambiguous records.
  
  As a result of this function, the request_response dataframe becomes unique on unique_reference and local_id. This is important for updating, and linking to, the mps_diagnostics table.
  '''
  w_ordered = Window.partitionBy([DATASET_ID_COL, UNIQUE_REFERENCE_COL, LOCAL_ID_COL]).orderBy(RES_AS_AT_DATE_COL, REQ_AS_AT_DATE_COL, RES_CREATED_COL, REQ_CREATED_COL)
  w = Window.partitionBy([DATASET_ID_COL, UNIQUE_REFERENCE_COL, LOCAL_ID_COL])

  df_requests_and_responses = (
    df_requests_and_responses
    .dropDuplicates()
    .withColumn('row', F.row_number().over(w_ordered))
    .withColumn('max_row', F.max('row').over(w))
    .filter(
      F.when(F.col(DATASET_ID_COL).isin(UNMANAGED_DATASETS),
             ((F.col('row') == F.col('max_row')) & (F.col('max_row') == 1))
            )
       .otherwise(F.col('row') == F.col('max_row'))
    )
    .drop('row', 'max_row')
  )
    
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## FILTER FOR NEW RECORDS

# COMMAND ----------

def filter_for_new_records(df_requests_and_responses: DataFrame, df_mps_diagnostics_saved: DataFrame) -> DataFrame:
  '''
  Take the existing mps_diagnostics. Find the maximum RES_CREATED_DATE. Use this date as a filter for new records.
  If there is no existing data, set the filter threshold to 01/01/1800, otherwise the threshold would be null and the condition is false for all new records.
  '''
  
  threshold_date = (
    df_mps_diagnostics_saved
    .select(F.max(F.col(RES_CREATED_COL)).alias('threshold_date'))
    .withColumn('threshold_date', F.when(F.col('threshold_date').isNotNull(), F.col('threshold_date')).otherwise(F.lit(datetime(1800, 1, 1))))
  )  

  df_requests_and_responses_delta = (
    df_requests_and_responses
    .crossJoin(threshold_date)
    .filter(F.col(RES_CREATED_COL) > F.col('threshold_date'))
    .drop('threshold_date')
  )
  
  return df_requests_and_responses_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE NEW RECORDS

# COMMAND ----------

def dt_merge_unique(database, target_table, df_source):
  '''
  Merge data from a source dataframe to a target delta table on LOCAL_ID_COL and UNIQUE_REFERENCE_COL.
  
  If any duplicates are present, use the source data.
  '''
  
  # get delta tables and source dataframe
  dt_target = DeltaTable.forName(spark, f'{database}.{target_table}')
  
  # construct merge update dictionary
  dict_merge_update = {}
  
  for col in df_source.columns:
    dict_merge_update[col] = f'source.{col}'

  # execute merge
  (
    dt_target
    .alias('target')
    .merge(df_source.alias('source'), f'target.{LOCAL_ID_COL} == source.{LOCAL_ID_COL} and target.{UNIQUE_REFERENCE_COL} == source.{UNIQUE_REFERENCE_COL}')
    .whenMatchedUpdate(set = dict_merge_update)
    .whenNotMatchedInsert(values = dict_merge_update) 
    .execute()
  )
  
  return dt_target.toDF()

# COMMAND ----------

# MAGIC %md
# MAGIC ## VALIDATE_SCHEMA

# COMMAND ----------

def validate_schema(df, schema_dict):
  # initialise array to store column status
  col_status = []
  
  # loop through columns in schema dictionary
  for key in schema_dict:
    col_name = schema_dict[key]['NAME']
    key_list = schema_dict[key]['KEYS']
    
    # check columns
    if col_name.lower() not in [col_name.lower() for col_name in df.columns]:
      col_status.append((col_name, False, 'not found', None, False, None))
      continue
    elif col_name in df.columns:
      column, column_name_correct, column_note = col_name, True, 'n/a'
    else:
      column, column_name_correct, column_note = col_name, True, 'incorrect case'
      
    # skip key validation if key list is empty
    if not key_list:
      col_status.append((column, column_name_correct, column_note, None, column_name_correct, None))
      continue
      
    # initialise array to store key status and a count for number of keys missing
    key_status = []
    n_key_names_missing = 0
    
    # pull dictionaries out of arrays
    df_exploded = df.select(F.explode(col_name))
    
    # select all dictionary keys (explode function defaults to column name of 'col')
    df_keys_as_cols = df_exploded.select('col.*')
    
    # check column keys
    for key_name in key_list:
      if key_name.lower() not in [key_name.lower() for key_name in df_keys_as_cols.columns]:
        n_key_names_missing += 1
        
        key_status.append(
          {'key': key_name, 'key_name_correct': False, 'key_note': 'not found'}
        )
      elif key_name in df_keys_as_cols.columns:
        key_status.append(
          {'key': key_name, 'key_name_correct': True, 'key_note': 'n/a'}
        )
      else:
        key_status.append(
          {'key': key_name, 'key_name_correct': True, 'key_note': 'incorrect case'}
        )
       
    # summarise status of column and its keys
    if column_name_correct and n_key_names_missing == 0:
      column_and_keys_correct = True
    else:
      column_and_keys_correct = False
      
    # add column status to array
    col_status.append((column, column_name_correct, column_note, key_status, column_and_keys_correct, n_key_names_missing))
  
  # convert array to dataframe
  df_col_status = spark.createDataFrame(col_status, schema=['column', 'column_name_correct', 'column_note', 'keys', 'column_and_keys_correct', 'n_key_names_missing'])
  
  
  return df_col_status

# COMMAND ----------

def schema_correct(df_schema_status):
  return df_schema_status.agg(F.min('column_and_keys_correct')).head()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## PERSON_ID

# COMMAND ----------

# For live data this would be added by the DPS pipeline. But we need this function for mps archive data.
# For one time use IDs, we add "U" as a placeholder for further derivations and sanity checks, but it will be removed at the end of the pipeline so that users will see NULL and refer to the Person ID column they received from MPS instead.

def add_person_id(df_requests_and_responses:DataFrame) -> DataFrame:
  df_requests_and_responses = df_requests_and_responses.withColumn(PERSON_ID_COL,
                                         F.when(
                                           (F.col(RES_MATCHED_NHS_NO_COL) != '') &
                                           (F.col(RES_MATCHED_NHS_NO_COL).isNotNull()) &
                                           (F.col(RES_MATCHED_NHS_NO_COL) != '0000000000') &
                                           (F.col(RES_MATCHED_NHS_NO_COL) != '9999999999'),
                                           F.col(RES_MATCHED_NHS_NO_COL)
                                         )
                                         .when(
                                           (F.col(RES_MPS_ID_COL) != '') &
                                           (F.col(RES_MPS_ID_COL).isNotNull()),
                                           F.split(F.col(RES_MPS_ID_COL), '~~~').getItem(0)
                                         )
                                         .otherwise('U') 
                                         .cast(StringType())
                                        )
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## PERSON_ID_TYPE

# COMMAND ----------

# NHS number is 10 digits

def add_person_id_type(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Adds a derived field to show which of the three person ID types has been output by MPS.
  - NHSNUMBER: if one is returned from PDS live or cached.
  - MPS_ID: from the MPS record bucket, or created new if the minimum required fields are present for an MPS_ID.
  - ONE_TIME_USE_ID: if neither of the above was found.
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(PERSON_ID_TYPE_COL,
                                         F.when(
                                           F.col(PERSON_ID_COL).rlike("^[0-9]{10}$"),
                                           'NHSNUMBER'
                                         )
                                         .when(
                                           F.col(PERSON_ID_COL).startswith('U'),
                                           'ONE_TIME_USE_ID'
                                         )
                                         .when(
                                           (F.col(PERSON_ID_COL).startswith('A')) |
                                           (F.col(PERSON_ID_COL).startswith('B')),
                                           'MPS_ID'
                                         )
                                         .otherwise(None)
                                         .cast(StringType())
                                        )
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## MPS_LAST_STEP_ATTEMPTED

# COMMAND ----------

def add_mps_last_step_attempted(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Adds the name of the last trace step attempted by MPS.
  Only includes the trace steps which look for a matching NHS number in PDS. Does not include whether MPS_ID matching was attempted.
  - CCT cached
  - CCT live
  - Alphanumeric trace
  - Algorithmic trace
  - No PDS tracing run
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(MPS_LAST_STEP_ATTEMPTED_COL,
                                         F.when(
                                           (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 1) &
                                           (F.col(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL).isNull()),
                                           'CCT_cached'
                                         )
                                         .when(
                                           (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 1) &
                                           (F.col(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL) == 0),
                                           'CCT_live'
                                         )
                                         .when(
                                           F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 3,
                                           'alphanumeric_trace_live'
                                         )
                                         .when(
                                           F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 4,
                                           'algorithmic_trace_live'
                                         )
                                         .when(
                                           F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 0,
                                           'No_PDS_tracing_run'
                                         )
                                         .otherwise(None)
                                         .cast(StringType())
                                        )
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## MPS_SUCCESSFUL_STEP

# COMMAND ----------

def add_mps_successful_step(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Shows the name of the successful trace step where MPS has matched to an NHS number. The trace steps are:
  - CCT cached
  -CCT live
  -Alphanumeric trace live
  -Algorithmic trace live
  -Not applicable (no trace step attempted or no match found)
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(MPS_SUCCESSFUL_STEP_COL,
                                           F.when(
                                             (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 1) & 
                                             (F.col(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL) == 100) & 
                                             (F.col(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL).isNull()), 'CCT_cached'
                                           )
                                          .when(
                                            (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 1) & 
                                            (F.col(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL) == 100) & 
                                            (F.col(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL) == 0), 'CCT_live'
                                          )
                                          .when(                                            
                                            (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 3) & 
                                            (F.col(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL) == 100), 'alphanumeric_trace_live'
                                          )
                                          .when(
                                            (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 4) & 
                                            (F.col(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL) >= 50), 'algorithmic_trace_live'
                                          )
                                          .when(
                                            (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) == 0), 'No_PDS_tracing_run'
                                          ) 
                                          .when(
                                            (F.col(RES_MATCHED_ALGORITHM_INDICATOR_COL) != 0) & 
                                            (F.col(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL) == 0), 'No_PDS_match_found'
                                          )                                      
                                          .otherwise(None)
                                          .cast(StringType())
                                         )
  
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## PDS_MATCH_FLAG

# COMMAND ----------

def add_pds_match_flag(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Flags if a match was made to an NHS number from PDS. 
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(PDS_MATCH_FLAG_COL,
                                                                   F.when(
                                                                     (F.col(PERSON_ID_TYPE_COL) == 'NHSNUMBER'), True
                                                                   )
                                                                   .otherwise(False)
                                                                  )
  
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## SUPERSEDED_NHS_NUMBER_FLAG

# COMMAND ----------

def explode_pds_by_replacementof(df_pds_full) -> DataFrame:
  '''
  From PDS full, make an exploded table where there is a row for every entry in the replacementOf column.
  This function is used in add_superseded_nhs_number_flag and add_nhs_number_history_list.
  '''
  df_pds_exploded_by_replacementof = (
    df_pds_full
    .select(PDS_SCHEMA['NHS_NUMBER_COL']['NAME'],
            F.explode(F.col(PDS_SCHEMA['REPLACEMENT_OF_COL']['NAME'])).alias('replacementOf_exploded')
           )
    .withColumn('replacementOf_value',
                F.col('replacementOf_exploded').getItem(PDS_KEYS['VALUE_KEY'])
               )
    .withColumn('replacementOf_start_date',
                F.when(F.col('replacementOf_exploded')[PDS_KEYS['SED_LOW_KEY']].isNotNull(),
                       F.to_date(F.col('replacementOf_exploded').getItem(PDS_KEYS['SED_LOW_KEY']), 'yyyyMMddHHmmss')
                      )
                .when(F.col('replacementOf_exploded')[PDS_KEYS['FROM_KEY']].isNotNull(),
                      F.to_date(F.col('replacementOf_exploded').getItem(PDS_KEYS['FROM_KEY']).cast('string'), 'yyyyMMdd')
                     )
                .otherwise(None)
               )
    .withColumn('replacementOf_end_date',
                F.when(F.col('replacementOf_exploded')[PDS_KEYS['SED_HIGH_KEY']].isNotNull(),
                       F.to_date(F.col('replacementOf_exploded').getItem(PDS_KEYS['SED_HIGH_KEY']), 'yyyyMMddHHmmss')
                      )
                .when(F.col('replacementOf_exploded')[PDS_KEYS['TO_KEY']].isNotNull(),
                      F.to_date(F.col('replacementOf_exploded').getItem(PDS_KEYS['TO_KEY']).cast('string'), 'yyyyMMdd')
                     )
                .otherwise(None)
               )
    .filter(F.col('replacementOf_value').rlike('^[0-9]{10}$'))
    .select(PDS_SCHEMA['NHS_NUMBER_COL']['NAME'], 'replacementOf_value', 'replacementOf_start_date', 'replacementOf_end_date')
  )
            
  return df_pds_exploded_by_replacementof

# COMMAND ----------

def add_request_date_col(df_requests_and_responses) -> DataFrame:
  '''
  Create a temporary "request_date" column which is the req_AS_AT_DATE if it is populated, or the req_CREATED if req_AS_AT_DATE is null.
  This function is used in add_superseded_nhs_number_flag and add_nhs_number_history_list.
  '''
  
  df_requests_and_responses = (
    df_requests_and_responses
      .withColumn('request_date', 
                  F.when(F.col(REQ_AS_AT_DATE_COL).isNotNull(), F.to_date(F.col(REQ_AS_AT_DATE_COL).cast('string'), 'yyyyMMdd'))
                  .when(F.col(REQ_CREATED_COL).isNotNull(), F.to_date(F.col(REQ_CREATED_COL), 'yyyy-MM-dd HH:mm:ss'))
                  .otherwise(None)
               )
  )
  
  return df_requests_and_responses

# COMMAND ----------

# These conditions are used in add_superseded_nhs_number_flag and/or add_nhs_number_history_list.

condition_request_date_in_replamcementOf_date_range = (
  ((F.col('request_date') > F.col('replacementOf_start_date')) | F.col('replacementOf_start_date').isNull()) &
  ((F.col('request_date') <= F.col('replacementOf_end_date')) | F.col('replacementOf_end_date').isNull())
)

condition_request_date_after_replamcementOf_SEDlow_date = (
  ((F.col('request_date') > F.col('replacementOf_start_date')) | F.col('replacementOf_start_date').isNull())
)

condition_request_nhs_no_matches_pds_replacement_of_value = (
  F.col(REQ_NHS_NO_COL) == F.col('replacementOf_value')
)

condition_response_nhs_no_matches_pds_nhs_no = (
  F.col(RES_MATCHED_NHS_NO_COL) == F.col(PDS_SCHEMA['NHS_NUMBER_COL']['NAME'])
)

# COMMAND ----------

def add_superseded_nhs_number_flag(df_requests_and_responses:DataFrame, df_pds_full:DataFrame) -> DataFrame:
  '''
  Flags if the response NHS number supersedes the request NHS number.
  
  From PDS full, make an exploded table where there is a row for every entry in the replacementOf column.
  Join the request and response table to this, with conditions that:
    1. the nhs number in PDS matches the response NHS number in MPS archive
    2. the replacementOf matches the request NHS number in MPS archive
    3. the AS_AT_date (or req_CREATED date if AS_AT_DATE is null) in MPS archive is later than replacementOf_start_date.
    4. the AS_AT_date (or req_CREATED date if AS_AT_DATE is null) in MPS archive is before replacementOf_end_date, unless replacementOf_end_date is null.
         This condition ensures that this column is derived consistently regardless of when it is run.
  Group by and aggregate on all request and response columns (this also has the effect of dropping the PDS columns and the temporary "request_date" column).
  Take the max of the flags (i.e. True if any are True).
  '''
  
  df_pds_exploded_by_replacementof = explode_pds_by_replacementof(df_pds_full)
  
  df_requests_and_responses = (
    add_request_date_col(df_requests_and_responses)
    .join(
      df_pds_exploded_by_replacementof,
      (
        condition_response_nhs_no_matches_pds_nhs_no &
        condition_request_nhs_no_matches_pds_replacement_of_value &
        condition_request_date_in_replamcementOf_date_range
      ),
      'left'    
    )
    .withColumn('superseded_nhs_number_flag_this_exploded_row', F.col(PDS_SCHEMA['NHS_NUMBER_COL']['NAME']).isNotNull())
    .groupBy(df_requests_and_responses.schema.names)
    .agg(F.max(F.col('superseded_nhs_number_flag_this_exploded_row')).alias(SUPERSEDED_NHS_NUMBER_FLAG_COL))
  )
  
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## NHS_NUMBER_HISTORY_LIST

# COMMAND ----------

def add_nhs_number_history_list(df_requests_and_responses:DataFrame, df_pds_full:DataFrame) -> DataFrame:
  '''
  List all NHS numbers which are superseded by the NHS number in the response table.
  
  From PDS full, make an exploded table where there is a row for every entry in the replacementOf column.
  Join the request and response table to this, with conditions that:
    1. the nhs number in PDS matches the response NHS number in MPS archive
    2. the AS_AT_date (or req_CREATED date if AS_AT_DATE is null) in MPS archive is later than replacementOf_start_date.
         This condition ensures that this column is derived consistently regardless of when it is run.
  Create the list of nhs numbers by collecting the replacementOf values when windowing over the request and response columns and ordering by replacementOf_start_date.
  Group by all request and response columns (this also has the effect of dropping the PDS columns and the temporary "request_date" column). 
  Aggregate by the max of the nhs number history lists.
  '''
  
  # An alias is needed for df_pds_exploded_by_replacementof because it references the same df_pds_full as its 'origin'. 
  # Therefore when it comes to joining this dataframe, the columns in the join condition need to be specifically referenced 
  # using its alias. This is only needed for older versions of Spark, but ensures backwards-compatibility.
  
  df_pds_exploded_by_replacementof = explode_pds_by_replacementof(df_pds_full).alias('df_pds_exploded_by_replacementof')

  w = Window.partitionBy(df_requests_and_responses.schema.names).orderBy('replacementOf_start_date')

  df_requests_and_responses = (
    add_request_date_col(df_requests_and_responses)
    .join(
      df_pds_exploded_by_replacementof,
      (
        condition_response_nhs_no_matches_pds_nhs_no &
        condition_request_date_after_replamcementOf_SEDlow_date
      ),
      'left'
    )
    .withColumn(NHS_NUMBER_HISTORY_LIST_COL, F.collect_list(F.col('replacementOf_value')).over(w))
    .groupBy(df_requests_and_responses.schema.names)
    .agg(F.array_distinct(F.max(NHS_NUMBER_HISTORY_LIST_COL)).alias(NHS_NUMBER_HISTORY_LIST_COL))
  )
   
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## MULTIPLE_PDS_MATCHES_FLAG

# COMMAND ----------

def add_multiple_pds_matches_flag(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Adds a flag if the query returned multiple matches from PDS.
  Without this, a user receiving a one-time-use ID may assume it was because there was insufficient data to find a match on PDS.
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(MULTIPLE_PDS_MATCHES_FLAG_COL, 
                                         F.when(
                                           (F.col(RES_ERROR_SUCCESS_CODE_COL) == '97'),
                                           True
                                         )
                                         .otherwise(
                                           False
                                         )
                                        )
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## MULTIPLE_MPS_ID_MATCHES_FLAG

# COMMAND ----------

def add_multiple_mps_id_matches_flag(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Adds a flag if the query returned multiple MPS_ID matches.
  If there are multiple MPS_ID matches, the PERSON_ID is populated with only one of them, so without this flag a user would not know that there was ambiguity in the match.
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(MULTIPLE_MPS_ID_MATCHES_FLAG_COL,
                                         F.when(
                                           F.size(F.split(F.col(RES_MPS_ID_COL), '~~~')) > 1,
                                           True
                                         )
                                         .otherwise(
                                           False
                                         )
                                        )
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ##MPS_MATCH_SCORE

# COMMAND ----------

def add_mps_match_score(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Copies the aggregated score given by MPS.
  This does not represent a probabilistic measure of confidence.
  '''
  df_requests_and_responses = df_requests_and_responses.withColumn(MPS_MATCH_SCORE_COL,
                                                                   F.col(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL).cast(DecimalType(5,2))
                                                                  )
  
  return df_requests_and_responses

# COMMAND ----------

# MAGIC %md
# MAGIC ## MPS_ALGORITHMIC_MATCH_SCORE

# COMMAND ----------

def add_mps_algorithmic_match_score(df_requests_and_responses:DataFrame) -> DataFrame:
  '''
  Copies the scoring given by MPS algorithmic trace to each demographic attribute.
  '''
  df_requests_and_responses = (
    df_requests_and_responses
    .withColumn(FAMILYNAME_ALGORITHMIC_MATCH_SCORE_COL, F.col(RES_ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC_COL).cast(DecimalType(5,2)))
    .withColumn(GIVENNAME_ALGORITHMIC_MATCH_SCORE_COL, F.col(RES_ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC_COL).cast(DecimalType(5,2)))
    .withColumn(DATEOFBIRTH_ALGORITHMIC_MATCH_SCORE_COL, F.col(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL).cast(DecimalType(5,2)))
    .withColumn(GENDER_ALGORITHMIC_MATCH_SCORE_COL, F.col(RES_ALGORITHMIC_TRACE_GENDER_SCORE_PERC_COL).cast(DecimalType(5,2)))
    .withColumn(POSTCODE_ALGORITHMIC_MATCH_SCORE_COL, F.col(RES_ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC_COL).cast(DecimalType(5,2)))
  )                                                          
  
  return df_requests_and_responses