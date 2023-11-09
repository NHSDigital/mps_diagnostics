# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('db', 'mps_diagnostics', 'db')
DB = dbutils.widgets.get('db')
assert DB

dbutils.widgets.text('uplift_notebook', '', 'uplift_notebook')
UPLIFT_NOTEBOOK = dbutils.widgets.get('uplift_notebook')

# COMMAND ----------

# MAGIC %run ./notebooks/imports

# COMMAND ----------

# MAGIC %run ./notebooks/config

# COMMAND ----------

# MAGIC %run ./schemas/sanity_checks_log

# COMMAND ----------

# MAGIC %run ./schemas/mps_diagnostics

# COMMAND ----------

# MAGIC %run ./notebooks/CCL_copy

# COMMAND ----------

if UPLIFT_NOTEBOOK != '':
  dbutils.notebook.run('./notebooks/uplifts/' + UPLIFT_NOTEBOOK, 0)

# COMMAND ----------

util.create_table_from_schema(spark=spark, schema=SANITY_CHECKS_LOG_SCHEMA, db_or_asset=DB, table=MPS_SANITY_CHECK_LOG_TABLE_NAME, overwrite=True)

# COMMAND ----------

create_partitioned_table_from_schema(spark=spark, partition_by = DATASET_ID_COL, schema=MPS_DIAGNOSTICS_SCHEMA, db_or_asset=DB, table=MPS_DIAGNOSTICS_TABLE_NAME, overwrite=True)