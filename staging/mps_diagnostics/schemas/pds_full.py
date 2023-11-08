# Databricks notebook source
# This schema is used to generate dummy dataframes for use in unit tests
PDS_FULL_SCHEMA = StructType([
  StructField(PDS_SCHEMA['NHS_NUMBER_COL']['NAME'], StringType()),
  StructField(PDS_SCHEMA['REPLACEMENT_OF_COL']['NAME'], ArrayType(
    StructType([
      StructField(PDS_KEYS['FROM_KEY'], IntegerType()),
      StructField(PDS_KEYS['TO_KEY'], IntegerType()),
      StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
      StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
      StructField(PDS_KEYS['VALUE_KEY'], StringType())
    ])
  )),
  StructField(PDS_SCHEMA['CONFIDENTIALITY_CODE_COL']['NAME'], ArrayType(
    StructType([
      StructField(PDS_KEYS['FROM_KEY'], IntegerType()),
      StructField(PDS_KEYS['TO_KEY'], IntegerType()),
      StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
      StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
      StructField(PDS_KEYS['CODE_KEY'], StringType())
    ])
  )),
  StructField(PDS_SCHEMA['CONFIDENTIALITY_CODE_HISTORY_COL']['NAME'], ArrayType(
    StructType([
      StructField(PDS_KEYS['FROM_KEY'], IntegerType()),
      StructField(PDS_KEYS['TO_KEY'], IntegerType()),
      StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
      StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
      StructField(PDS_KEYS['CODE_KEY'], StringType())
    ])
  ))
])