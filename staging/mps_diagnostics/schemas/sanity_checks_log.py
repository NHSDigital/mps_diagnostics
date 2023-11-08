# Databricks notebook source
SANITY_CHECKS_LOG_SCHEMA = StructType([
  StructField('run_id', IntegerType()),
  StructField('number_records_in_delta', IntegerType()),
  StructField('test_stage', StringType()),
  StructField('sanity_check', StringType()),
  StructField('assumption_valid', BooleanType()),
  StructField('details', StringType()),
  StructField('time_run_completed', TimestampType())
])