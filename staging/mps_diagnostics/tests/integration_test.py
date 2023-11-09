# Databricks notebook source
# MAGIC %md
# MAGIC The integration test serves two purposes.
# MAGIC 
# MAGIC Firstly, to test that the main notebook runs successfully in its entirety.
# MAGIC 
# MAGIC Secondly, to test that the mps diagnostic columns contain examples of all the possible values we are expecting. This check can be carried out by inspecting the results in this notebook.

# COMMAND ----------

# MAGIC %run ../notebooks/config

# COMMAND ----------

# MAGIC %run ../notebooks/imports

# COMMAND ----------

# Drop tables from the test database if they already exist.
# So they can be created again in this test.

if util.table_exists(spark = spark, db = TEST_DATABASE_NAME, table = MPS_DIAGNOSTICS_TABLE_NAME):
  util.drop_table(spark = spark, db = TEST_DATABASE_NAME, table = MPS_DIAGNOSTICS_TABLE_NAME)
  
VIEW_NAMES = [v[0] for v in spark.table(f'{REQUESTS_AND_RESPONSES_DATABASE_NAME}.{REQUESTS_AND_RESPONSES_TABLE_NAME}').select(DATASET_ID_COL).distinct().collect()]
  
for view_name in VIEW_NAMES:
  if util.table_exists(spark = spark, db = TEST_DATABASE_NAME, table = view_name):
    spark.sql(f'DROP VIEW {TEST_DATABASE_NAME}.{view_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## RUN INIT_SCHEMAS AND MAIN ON THE TEST DATABASE

# COMMAND ----------

dbutils.notebook.run('../init_schemas', 0, arguments={'db': TEST_DATABASE_NAME})

# COMMAND ----------

dbutils.notebook.run('../notebooks/main', 0, arguments={'db': TEST_DATABASE_NAME})

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHECK MPS_DIAGNOSTICS OUTPUTS

# COMMAND ----------

df_mps_diagnostics = spark.table(TEST_DATABASE_NAME + '.' + MPS_DIAGNOSTICS_TABLE_NAME)

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(PERSON_ID_TYPE_COL)).count())

# expected:
# NHSNUMBER
# MPS_ID
# ONE_TIME_USE_ID
# null

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_LAST_STEP_ATTEMPTED_COL)).count())

# expected:
# CCT_cached (wouldn't apply to the mps_archive input)
# CCT_live
# alphanumeric_trace_live
# algorithmic_trace_live
# No_PDS_tracing_run
# null

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_SUCCESSFUL_STEP_COL)).count())

# expected:
# CCT_cached (wouldn't apply to the mps_archive input)
# CCT_live
# alphanumeric_trace_live
# algorithmic_trace_live
# No_PDS_tracing_run
# No_PDS_match_found
# null

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(PDS_MATCH_FLAG_COL)).count())

# expected:
# true
# false

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(SUPERSEDED_NHS_NUMBER_FLAG_COL)).count())

# expected:
# true
# false

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.size(F.col(NHS_NUMBER_HISTORY_LIST_COL))).count())

# expected:
# 0
# 1
# 2
# 3
# etc...

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MULTIPLE_PDS_MATCHES_FLAG_COL)).count())

# expected:
# true
# false

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MULTIPLE_MPS_ID_MATCHES_FLAG_COL)).count())

# expected:
# true
# false

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_MATCH_SCORE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(FAMILYNAME_ALGORITHMIC_MATCH_SCORE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(GIVENNAME_ALGORITHMIC_MATCH_SCORE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(DATEOFBIRTH_ALGORITHMIC_MATCH_SCORE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(GENDER_ALGORITHMIC_MATCH_SCORE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(POSTCODE_ALGORITHMIC_MATCH_SCORE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## CHECK VIEWS

# COMMAND ----------

for view_name in VIEW_NAMES:
  
  view_df = spark.table(TEST_DATABASE_NAME + '.' + view_name)
  df_mps_diagnostics_filtered = df_mps_diagnostics.filter(F.col(DATASET_ID_COL) == view_name)
  
  view_dataset_values = view_df.select(F.col(DATASET_ID_COL)).distinct().collect()
  view_dataset_values = [v[DATASET_ID_COL] for v in view_dataset_values]
  
  view_count = view_df.count()
  filtered_df_count = df_mps_diagnostics_filtered.count()
  
  print(f'View {view_name} has {len(view_dataset_values)} value(s) for {DATASET_ID_COL}: {view_dataset_values}. The view contains {view_count} records, compared to {filtered_df_count} records that appear in the main mps_diagnostics table filtered on the appropriate {DATASET_ID_COL}.')
  
  