# Databricks notebook source
import os

# COMMAND ----------

dbutils.notebook.run('./tests/utils_test', 0)

# the following test requires the creation of a table, so is possible only in the REF environment.
if os.environ['env'] == 'ref':
  dbutils.notebook.run('./tests/utils_ref_test', 0)