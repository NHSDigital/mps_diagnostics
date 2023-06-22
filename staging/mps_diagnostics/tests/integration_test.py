# Databricks notebook source
# MAGIC %run ../notebooks/config

# COMMAND ----------

# MAGIC %run ../notebooks/imports

# COMMAND ----------

TEST_DATABASE_NAME = 'testdata_mps_diagnostics_mps_diagnostics'

# COMMAND ----------

if util.table_exists(spark = spark, db = TEST_DATABASE_NAME, table = MPS_DIAGNOSTICS_TABLE_NAME):
  util.drop_table(spark = spark, db = TEST_DATABASE_NAME, table = MPS_DIAGNOSTICS_TABLE_NAME)

# COMMAND ----------

dbutils.notebook.run('../init_schemas', 0, arguments={'db': TEST_DATABASE_NAME})

# COMMAND ----------

dbutils.notebook.run('../notebooks/main', 0, arguments={'db': TEST_DATABASE_NAME})

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

display(df_mps_diagnostics.groupBy(F.col(MPS_ALGORITHMIC_MATCH_SCORE_COL + '.' + FAMILYNAME_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_ALGORITHMIC_MATCH_SCORE_COL + '.' + GIVENNAME_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_ALGORITHMIC_MATCH_SCORE_COL + '.' + DATEOFBIRTH_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_ALGORITHMIC_MATCH_SCORE_COL + '.' + GENDER_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100

# COMMAND ----------

display(df_mps_diagnostics.groupBy(F.col(MPS_ALGORITHMIC_MATCH_SCORE_COL + '.' + POSTCODE_COL)).count())

# expected:
# 0
# 100
# values between 0 and 100