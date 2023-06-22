# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('notebook_root', '/staging/mps_diagnostics/run_notebooks', 'notebook_root')
dbutils.widgets.text('db', 'mps_diagnostics', 'db')

DB = dbutils.widgets.get('db')
assert DB

NOTEBOOK_ROOT = dbutils.widgets.get('notebook_root')
assert NOTEBOOK_ROOT

# COMMAND ----------

# MAGIC %run ./notebooks/imports

# COMMAND ----------

start_time = datetime.now()
print(f'BEGINNING MPS DIAGNOSTICS PIPELINE EXECUTION @ {start_time}')

# COMMAND ----------

dbutils.notebook.run('./notebooks/main', 0, arguments={'db': DB})

# COMMAND ----------

end_time = datetime.now()
run_time = end_time - start_time
print(f'Time to complete pipeline = {run_time}')