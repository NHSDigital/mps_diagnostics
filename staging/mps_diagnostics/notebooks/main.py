# Databricks notebook source
# MAGIC %md
# MAGIC This notebook runs the pipeline to generates mps_diagnostics, and the sanity checks.

# COMMAND ----------

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
# MAGIC ## LOAD DATA

# COMMAND ----------

df_requests_and_responses = spark.table(REQUESTS_AND_RESPONSES_DATABASE_NAME + '.' + REQUESTS_AND_RESPONSES_TABLE_NAME) #.limit(1000).cache()

df_requests_and_responses = remove_duplicate_request_response_records(df_requests_and_responses, UNMANAGED_DATASETS=UNMANAGED_DATASETS)

# some of the mps diagnostics are derived from PDS, so we need to load a PDS table.
df_pds_full = spark.table(PDS_FULL_DATABASE_NAME + '.' + PDS_FULL_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VALIDATE PDS INPUT SCHEMA

# COMMAND ----------

df_pds_schema_status = validate_schema(df_pds_full, PDS_SCHEMA)
assert schema_correct(df_pds_schema_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHOOSE COLUMNS TO DROP FROM MPS DIAGNOSTICS

# COMMAND ----------

# df_mps_diagnostics_delta will be constructed by adding columns to df_request_response_delta.
# here we decide which columns of df_request_response_delta can be dropped from df_mps_diagnostics.

COLS_TO_KEEP = [
  DATASET_ID_COL, LOCAL_ID_COL, UNIQUE_REFERENCE_COL,
  REQ_CREATED_COL, REQ_AS_AT_DATE_COL,
  RES_CREATED_COL, RES_AS_AT_DATE_COL
]

COLS_TO_DROP = [x for x in df_requests_and_responses.schema.names if x not in COLS_TO_KEEP]

# COMMAND ----------

# MAGIC %md
# MAGIC ## FILTER FOR NEW RECORDS

# COMMAND ----------

# load existing dataset and filter df_requests_and_responses for records that were added more recently.

df_mps_diagnostics_saved = spark.table(DB + '.' + MPS_DIAGNOSTICS_TABLE_NAME)
df_requests_and_responses_delta = filter_for_new_records(df_requests_and_responses, df_mps_diagnostics_saved)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE TEMP_REQUEST_RESPONSE_DELTA TABLE

# COMMAND ----------

# the sanity check notebook runs sanity checks on the request response delta table.
# it is quicker to write this to a temporary table and read it in the sanity check notebook, than to pass a dataframe to the sanity check notebook.

util.create_table(
    spark=spark,
    df=df_requests_and_responses_delta,
    db_or_asset= DB + '.' + REQUESTS_AND_RESPONSES_DELTA_TABLE_NAME,
    overwrite=True
    #owner=DB     #this doesn't work with DB=testdata_mps_diagnostics_mps_diagnostics
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADD MPS DIAGNOSTICS

# COMMAND ----------

df_requests_and_responses_delta = df_requests_and_responses_delta.withColumn(MATCHED_NHS_NO_COL, F.col(RES_MATCHED_NHS_NO_COL))

df_requests_and_responses_delta = add_person_id(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_person_id_type(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_last_step_attempted(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_successful_step(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_pds_match_flag(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_superseded_nhs_number_flag(df_requests_and_responses_delta, df_pds_full)
df_requests_and_responses_delta = add_nhs_number_history_list(df_requests_and_responses_delta, df_pds_full)
df_requests_and_responses_delta = add_multiple_pds_matches_flag(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_multiple_mps_id_matches_flag(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_match_score(df_requests_and_responses_delta)
df_requests_and_responses_delta = add_mps_algorithmic_match_score(df_requests_and_responses_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAVE TEMP_MPS_DIAGNOSTICS_DELTA TABLE

# COMMAND ----------

# the sanity check notebook runs sanity checks on the mps diagnostics delta table.
# it is quicker to write this to a temporary table and read it in the sanity check notebook, than to pass a dataframe to the sanity check notebook.

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

# N.B. The delta tables that are run through the sanity checks are saved before any duplicate records have been identified or removed.
# This means that the sanity checks will run on all new records, whether they are duplicates or not.

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP TEMP TABLES

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
# MAGIC ## DROP UNWANTED COLUMNS AND POSTPROCESSING

# COMMAND ----------

# Drop the columns that were part of df_request_response_delta but are not wanted in df_mps_diagnostics.
# Remove "U" Person IDs. (these were a placeholder for sanity checks on one-time-use ID). For the final asset we don't know what the OTUID would be, so better to leave it blank.
# Add a timestamp for when these records were processed.
# Change some column types. The request and response file has some dates as Integers, which we would prefer to store as Dates in mps_diagnostics.

df_mps_diagnostics_delta = (
  df_requests_and_responses_delta
  .drop(*COLS_TO_DROP)
  .withColumn(PERSON_ID_COL,
              F.when(F.col(PERSON_ID_COL) == 'U', F.lit(None))
               .otherwise(F.col(PERSON_ID_COL))
             )
  .withColumn(MPS_DIAGNOSTICS_TIMESTAMP_COL, F.lit(datetime.now()).cast('timestamp'))
  .withColumn(REQ_AS_AT_DATE_COL, F.to_date(F.col(REQ_AS_AT_DATE_COL).cast('string')))
  .withColumn(RES_AS_AT_DATE_COL, F.to_date(F.col(RES_AS_AT_DATE_COL).cast('string')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE DELTA TABLE INTO SAVED TABLE

# COMMAND ----------

dt_merge_unique(DB, MPS_DIAGNOSTICS_TABLE_NAME, df_mps_diagnostics_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE VIEWS FOR SELECTED DATASET IDS

# COMMAND ----------

# New data is often written in very small files, which can potentially slow down queries. The solution is to compact many small files into one larger one, using the OPTIMIZE function.

try:
  spark.sql(f'OPTIMIZE {DB}.{MPS_DIAGNOSTICS_TABLE_NAME}')
except:
  print(f'Could not optimize {DB}.{MPS_DIAGNOSTICS_TABLE_NAME}')

# COMMAND ----------

VIEW_NAMES = [v[0] for v in df_requests_and_responses.select(DATASET_ID_COL).distinct().collect()]

for view_name in VIEW_NAMES:
  
  spark.sql(
    f'CREATE VIEW IF NOT EXISTS {DB}.{view_name} AS '
    f'SELECT * FROM {DB}.{MPS_DIAGNOSTICS_TABLE_NAME} '
    f'WHERE {DATASET_ID_COL} = "{view_name}"'
  )