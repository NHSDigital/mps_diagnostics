# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('db', 'mps_diagnostics', 'db')
DB = dbutils.widgets.get('db')
assert DB

# COMMAND ----------

# MAGIC %run ./notebooks/imports

# COMMAND ----------

# MAGIC %run ./notebooks/config

# COMMAND ----------

sanity_check_log_schema = StructType([
    StructField('Test_Stage', StringType(), True),
    StructField('Sanity_Check', StringType(), True),
    StructField('Assumption_Valid', StringType(), True),
    StructField('Num_Records_Break_Assumption', IntegerType(), True),
    StructField('Time_Run_Completed', TimestampType(), True),
])
util.create_table_from_schema(spark=spark, schema=sanity_check_log_schema, db_or_asset=DB, table=MPS_SANITY_CHECK_LOG_TABLE_NAME, overwrite=True)

# COMMAND ----------

schema_mps_diagnostics = StructType([
  StructField('dataset_id', StringType(), False), # 0
  StructField('local_id', StringType(), False), # 0
  StructField('unique_reference', StringType(), False), # 0
  StructField('req_created', TimestampType(), True), # 0 
  StructField('req_AS_AT_DATE', DateType(), True), # 142005464
  StructField('res_created', TimestampType(), True), # 0
  StructField('res_AS_AT_DATE', DateType(), True), # 400333078
  StructField('res_MPS_ID', StringType(), True), # 423418061
  StructField('res_MATCHED_NHS_NO', StringType(), True), # 179720932
  StructField('PERSON_ID_TYPE', StringType(), True), # 0, NONE
  StructField('MPS_LAST_STEP_ATTEMPTED', StringType(), True), # 0, NONE
  StructField('MPS_SUCCESSFUL_STEP', StringType(), True), # 0, NONE
  StructField('PDS_MATCH_FLAG', BooleanType(), False), # 0, TRUE/FALSE
  StructField('SUPERSEDED_NHS_NUMBER_FLAG', BooleanType(), False), # 0, TRUE/FALSE
  StructField('NHS_NUMBER_HISTORY_LIST', ArrayType(StringType()), True), # 0, NONE
  StructField('MULTIPLE_PDS_MATCHES_FLAG', BooleanType(), False), # 0, TRUE/FALSE
  StructField('MULTIPLE_MPS_ID_MATCHES_FLAG', BooleanType(), False), # 0, TRUE/FALSE
  StructField('MPS_MATCH_SCORE', DecimalType(5,2), True), # 0
  StructField('MPS_ALGORITHMIC_MATCH_SCORE', StructType([
    StructField('FAMILYNAME', DecimalType(5,2), True), 
    StructField('GIVENNAME', DecimalType(5,2), True), 
    StructField('DATEOFBIRTH', DecimalType(5,2), True), 
    StructField('GENDER', DecimalType(5,2), True), 
    StructField('POSTCODE', DecimalType(5,2), True)])), # 0
  StructField('MPS_DIAGNOSTICS_TIMESTAMP', TimestampType(), True), # 0
])

util.create_table_from_schema(spark=spark, schema=schema_mps_diagnostics, db_or_asset=DB, table=MPS_DIAGNOSTICS_TABLE_NAME, overwrite=True)