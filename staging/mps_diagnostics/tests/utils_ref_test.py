# Databricks notebook source
# MAGIC %md
# MAGIC This notebook contains a unit test which requires a table to be created.
# MAGIC 
# MAGIC The automated build cannot create test tables. Therefore this test is kept seperate so that it is to be run on the REF environment only.

# COMMAND ----------

# MAGIC %run ../notebooks/imports

# COMMAND ----------

# MAGIC %run ../notebooks/config

# COMMAND ----------

# MAGIC %run ../notebooks/CCL_copy

# COMMAND ----------

# MAGIC %run ../notebooks/utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## DT_MERGE_UNIQUE

# COMMAND ----------

dt_merge_unique_suite = FunctionTestSuite()


@dt_merge_unique_suite.add_test
def test_dt_merge_unique():
  columns = [LOCAL_ID_COL, UNIQUE_REFERENCE_COL, 'A', 'B']
  
  df_saved = spark.createDataFrame(
    [
      (0, 0, 0, 0),
      (0, 1, 2, 3)
    ],
    columns
  )

  df_delta = spark.createDataFrame(
    [
      (0, 0, 0, 1),
      (4, 5, 6, 7)
    ],
    columns
  )
  
  df_expected = spark.createDataFrame(
    [
      (0, 0, 0, 1),
      (0, 1, 2, 3),
      (4, 5, 6, 7)
    ],
    columns
  )

  spark.sql(f'USE {TEST_DATABASE_NAME}')
  spark.sql(f'DROP TABLE IF EXISTS {DT_MERGE_TEST_TABLE_NAME}')
  
  (
    df_saved
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'{TEST_DATABASE_NAME}.{DT_MERGE_TEST_TABLE_NAME}')
  )
  
  df_output = dt_merge_unique(TEST_DATABASE_NAME, DT_MERGE_TEST_TABLE_NAME, df_delta)
  assert compare_results(df_output, df_expected, join_columns = [LOCAL_ID_COL, UNIQUE_REFERENCE_COL])

  
dt_merge_unique_suite.run()