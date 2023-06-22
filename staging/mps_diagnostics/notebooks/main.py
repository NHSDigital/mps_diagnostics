# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('db', '', 'db')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD

# COMMAND ----------

df_requests_and_responses = spark.table(REQUESTS_AND_RESPONSES_DATABASE_NAME + '.' + REQUESTS_AND_RESPONSES_TABLE_NAME) #.limit(1000).cache()

df_requests_and_responses = remove_duplicate_request_response_records(df_requests_and_responses, UNMANAGED_DATASETS=UNMANAGED_DATASETS)

# for live data the person ID would be added by the DPS pipeline, but for mps archive data we need to add it.
df_requests_and_responses = add_person_id(df_requests_and_responses)

# some of the deterministic diagnostics are derived from PDS, so we need to load a PDS table.
df_pds_full = spark.table(PDS_FULL_DATABASE_NAME + '.' + PDS_FULL_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDATE SCHEMAS

# COMMAND ----------

df_pds_schema_status = validate_schema(df_pds_full, PDS_SCHEMA)
assert schema_correct(df_pds_schema_status)

# COMMAND ----------

COLS_TO_KEEP = [
  DATASET_ID_COL, LOCAL_ID_COL, UNIQUE_REFERENCE_COL,
  REQ_CREATED_COL, REQ_AS_AT_DATE_COL,
  RES_CREATED_COL, RES_AS_AT_DATE_COL,
 RES_MPS_ID_COL, RES_MATCHED_NHS_NO_COL
]

COLS_TO_DROP = [x for x in df_requests_and_responses.schema.names if x not in COLS_TO_KEEP]

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTER FOR NEW RECORDS

# COMMAND ----------

# load existing dataset and filter df_requests_and_responses for records that were added more recently.
df_mps_diagnostics_saved = spark.table(DB + '.' + MPS_DIAGNOSTICS_TABLE_NAME)
df_requests_and_responses_delta = filter_for_new_records(df_requests_and_responses, df_mps_diagnostics_saved)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE TEMP_REQUEST_RESPONSE_DELTA TABLE

# COMMAND ----------

util.create_table(
    spark=spark,
    df=df_requests_and_responses_delta,
    db_or_asset= DB + '.' + REQUESTS_AND_RESPONSES_DELTA_TABLE_NAME,
    overwrite=True
    #owner=DB     #this doesn't work with DB=testdata_mps_diagnostics_mps_diagnostics
  )

# COMMAND ----------

# MAGIC %md
# MAGIC # ADD DETERMINISTIC DIAGNOSTICS

# COMMAND ----------

df_requests_and_responses_delta = add_person_id_type(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_last_step_attempted(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_successful_step(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_pds_match_flag(df_requests_and_responses_delta)

df_requests_and_responses_delta = add_sensitive_flag(df_requests_and_responses_delta) # sensitive flag will be dropped from the final table, but still used for deriving NHS number history list.
df_requests_and_responses_delta = add_superseded_nhs_number_flag(df_requests_and_responses_delta, df_pds_full)
df_requests_and_responses_delta = add_nhs_number_history_list(df_requests_and_responses_delta, df_pds_full).drop(SENSITIVE_FLAG_COL)

df_requests_and_responses_delta = add_multiple_pds_matches_flag(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_multiple_mps_id_matches_flag(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_match_score(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_algorithmic_match_score(df_requests_and_responses_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE TEMP_MPS_DIAGNOSTICS_DELTA TABLE

# COMMAND ----------

util.create_table(
    spark=spark,
    df=df_requests_and_responses_delta,
    db_or_asset= DB + '.' + MPS_DIAGNOSTICS_DELTA_TABLE_NAME,
    overwrite=True
    #owner=DB     #this doesn't work with DB=testdata_mps_diagnostics_mps_diagnostics
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## RUN SANITY CHECKS

# COMMAND ----------

dbutils.notebook.run('./sanity_checks', 0, arguments={'db': DB})

# N.B. These delta tables that are run through the sanity checks are saved before any duplicate records have been identified or removed. This means that the sanity checks will run on all new records, whether they are duplicates or not.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELETE TEMP TABLES

# COMMAND ----------

util.drop_table(
    spark=spark,
    db=DB,
    table=REQUESTS_AND_RESPONSES_DELTA_TABLE_NAME
  )

# COMMAND ----------

util.drop_table(
    spark=spark,
    db=DB,
    table=MPS_DIAGNOSTICS_DELTA_TABLE_NAME
  )

# COMMAND ----------

# MAGIC %md
# MAGIC # DROP COLUMNS AND FORMAT SCHEMAS

# COMMAND ----------

df_mps_diagnostics_delta = (
  df_requests_and_responses_delta
  .drop(*COLS_TO_DROP)
  .withColumn(MPS_DIAGNOSTICS_TIMESTAMP_COL, F.lit(datetime.now()).cast('timestamp'))
)

df_mps_diagnostics_delta = format_schemas(df_mps_diagnostics_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC # CHECKING FOR DUPLICATES

# COMMAND ----------

#Check if the indexing is shared
df_records_with_shared_index = (
  df_mps_diagnostics_delta
  .join(df_mps_diagnostics_saved,
        (df_mps_diagnostics_delta[LOCAL_ID_COL] == df_mps_diagnostics_saved[LOCAL_ID_COL]) & (df_mps_diagnostics_delta[UNIQUE_REFERENCE_COL] == df_mps_diagnostics_saved[UNIQUE_REFERENCE_COL]),
        'inner'
  )
)

if df_records_with_shared_index.count() > 0:
  #Need to remove the table via anti join
  df_mps_diagnostics_duplicate_indices_removed = (
    df_mps_diagnostics_saved
    .join(df_mps_diagnostics_delta,
        (df_mps_diagnostics_delta[LOCAL_ID_COL] == df_mps_diagnostics_saved[LOCAL_ID_COL]) & (df_mps_diagnostics_delta[UNIQUE_REFERENCE_COL] == df_mps_diagnostics_saved[UNIQUE_REFERENCE_COL]),
         'left_anti'
         )
  )
    
  #Saving the data
  util.create_table(
    spark=spark,
    df=df_mps_diagnostics_duplicate_indices_removed,
    db_or_asset= DB + '.' + MPS_DIAGNOSTICS_TABLE_NAME,
    overwrite=True,
    #owner=DB     #this doesn't work with DB=testdata_mps_diagnostics_mps_diagnostics
  )

# COMMAND ----------

# MAGIC %md
# MAGIC # WRITE TO TABLE

# COMMAND ----------

util.append_to_table(spark,
                     df = df_mps_diagnostics_delta, 
                     id_cols = [LOCAL_ID_COL, UNIQUE_REFERENCE_COL],
                     output_loc= DB + '.' + MPS_DIAGNOSTICS_TABLE_NAME,
                     allow_nullable_schema_mismatch = True
                    )