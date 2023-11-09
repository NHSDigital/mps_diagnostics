# Databricks notebook source
MPS_DIAGNOSTICS_SCHEMA = StructType([
  StructField(DATASET_ID_COL, StringType()),
  StructField(LOCAL_ID_COL, StringType()),
  StructField(UNIQUE_REFERENCE_COL, StringType()),
  StructField(REQ_CREATED_COL, TimestampType()),
  StructField(REQ_AS_AT_DATE_COL, DateType()),
  StructField(RES_CREATED_COL, TimestampType()),
  StructField(RES_AS_AT_DATE_COL, DateType()),
  StructField(MATCHED_NHS_NO_COL, StringType()),
  StructField(PERSON_ID_COL, StringType()),
  StructField(PERSON_ID_TYPE_COL, StringType()),
  StructField(MPS_LAST_STEP_ATTEMPTED_COL, StringType()),
  StructField(MPS_SUCCESSFUL_STEP_COL, StringType()),
  StructField(PDS_MATCH_FLAG_COL, BooleanType()),
  StructField(SUPERSEDED_NHS_NUMBER_FLAG_COL, BooleanType()),
  StructField(NHS_NUMBER_HISTORY_LIST_COL, ArrayType(StringType())),
  StructField(MULTIPLE_PDS_MATCHES_FLAG_COL, BooleanType()),
  StructField(MULTIPLE_MPS_ID_MATCHES_FLAG_COL, BooleanType()),
  StructField(MPS_MATCH_SCORE_COL, DecimalType(5,2)),
  StructField(FAMILYNAME_ALGORITHMIC_MATCH_SCORE_COL, DecimalType(5,2)), 
  StructField(GIVENNAME_ALGORITHMIC_MATCH_SCORE_COL, DecimalType(5,2)), 
  StructField(DATEOFBIRTH_ALGORITHMIC_MATCH_SCORE_COL, DecimalType(5,2)), 
  StructField(GENDER_ALGORITHMIC_MATCH_SCORE_COL, DecimalType(5,2)), 
  StructField(POSTCODE_ALGORITHMIC_MATCH_SCORE_COL, DecimalType(5,2)),
  StructField(MPS_DIAGNOSTICS_TIMESTAMP_COL, TimestampType()),
])

# COMMAND ----------

