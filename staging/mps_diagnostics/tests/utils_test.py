# Databricks notebook source
# MAGIC %run ../notebooks/imports

# COMMAND ----------

# MAGIC %run ../notebooks/function_test_suite

# COMMAND ----------

# MAGIC %run ../notebooks/config

# COMMAND ----------

# MAGIC %run ../notebooks/utils

# COMMAND ----------

# MAGIC %md
# MAGIC #Ambiguous join test

# COMMAND ----------

ambiguous_join_test = FunctionTestSuite()

@ambiguous_join_test.add_test
def remove_duplicate_local_ids_and_unique_refs():
  
  df_input = spark.createDataFrame(
    [
      (1, 'adhoc', 123, 456, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (2, 'adhoc', 123, 456, datetime(2023,1,1), datetime(2023,1,1), datetime(2023,1,1), datetime(2023,1,1)),
      (3, 'adhoc', 234, 456, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (4, 'adhoc', 234, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (5, 'adhoc', 345, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (6, 'npex', 123, 456, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (7, 'npex', 123, 456, datetime(2023,1,1), datetime(2023,1,1), datetime(2023,1,1), datetime(2023,1,1)),
      (8, 'npex', 234, 456, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (9, 'npex', 234, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (10, 'npex', 345, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1))
    ],
    ['id', DATASET_ID_COL, LOCAL_ID_COL, UNIQUE_REFERENCE_COL, RES_AS_AT_DATE_COL, REQ_AS_AT_DATE_COL, RES_CREATED_COL, REQ_CREATED_COL]
  )
  
  df_expected = spark.createDataFrame(
    [
      (3, 'adhoc', 234, 456, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (4, 'adhoc', 234, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (5, 'adhoc', 345, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (7, 'npex', 123, 456, datetime(2023,1,1), datetime(2023,1,1), datetime(2023,1,1), datetime(2023,1,1)),
      (8, 'npex', 234, 456, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (9, 'npex', 234, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1)),
      (10, 'npex', 345, 789, datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1), datetime(2020,1,1))
    ],
    ['id', DATASET_ID_COL, LOCAL_ID_COL, UNIQUE_REFERENCE_COL, RES_AS_AT_DATE_COL, REQ_AS_AT_DATE_COL, RES_CREATED_COL, REQ_CREATED_COL]
  )
                
  df_output = remove_duplicate_request_response_records(df_input, UNMANAGED_DATASETS=['adhoc', 'mpsaas'])
  
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
ambiguous_join_test.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # FILTER FOR NEW RECORDS

# COMMAND ----------

filter_for_new_records_test_suite = FunctionTestSuite()

@filter_for_new_records_test_suite.add_test
def test_filter_new_records():
  
  df_input = spark.createDataFrame(
    [
      (4, datetime(2023, 1, 1, 9, 0, 0)),
      (5, datetime(2023, 1, 2, 5, 0, 0)),
      (6, datetime(2023, 1, 2, 9, 0, 0)),
      (7, datetime(2023, 1, 4, 9, 0, 0))
    ],
    StructType([
      StructField('id', IntegerType(), True),
      StructField(RES_CREATED_COL,TimestampType(),True),
    ])
  )
  
  df_existing = spark.createDataFrame(
    [
      (1, datetime(2023, 1, 1, 9, 0, 0)),
      (2, datetime(2023, 1, 2, 5, 0, 0)),
      (3, datetime(2023, 1, 2, 8, 0, 0))
    ],
    StructType([
      StructField('id', IntegerType(), True),
      StructField(RES_CREATED_COL,TimestampType(),True),
    ])
  )
  
  df_expected = spark.createDataFrame(
    [
      (6, datetime(2023, 1, 2, 9, 0, 0)),
      (7, datetime(2023, 1, 4, 9, 0, 0))
    ],
    StructType([
      StructField('id', IntegerType(), True),
      StructField(RES_CREATED_COL,TimestampType(),True),
    ])
  )
  
  df_output = filter_for_new_records(df_input, df_existing)
  assert compare_results(df_output, df_expected, join_columns = ['id'])

  
@filter_for_new_records_test_suite.add_test
def test_filter_new_records_from_blank():
  
  df_input = spark.createDataFrame(
    [
      (4, datetime(2023, 1, 1, 9, 0, 0)),
      (5, datetime(2023, 1, 2, 5, 0, 0)),
      (6, datetime(2023, 1, 2, 9, 0, 0)),
      (7, datetime(2023, 1, 4, 9, 0, 0))
    ],
    StructType([
      StructField('id', IntegerType(), True),
      StructField(RES_CREATED_COL,TimestampType(),True),
    ])
  )
  
  df_existing = spark.createDataFrame(
    [],
    StructType([
      StructField('id', IntegerType(), True),
      StructField(RES_CREATED_COL,TimestampType(),True),
    ])
  )
  
  df_expected = spark.createDataFrame(
    [
      (4, datetime(2023, 1, 1, 9, 0, 0)),
      (5, datetime(2023, 1, 2, 5, 0, 0)),
      (6, datetime(2023, 1, 2, 9, 0, 0)),
      (7, datetime(2023, 1, 4, 9, 0, 0))
    ],
    StructType([
      StructField('id', IntegerType(), True),
      StructField(RES_CREATED_COL,TimestampType(),True),
    ])
  )
  
  df_output = filter_for_new_records(df_input, df_existing)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
filter_for_new_records_test_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # PERSON_ID

# COMMAND ----------

person_id_test_suite = FunctionTestSuite()

@person_id_test_suite.add_test
def test_nhs_number():
  
  df_input = spark.createDataFrame(
    [
      (1, '1234567890', None),
      (2, '0000000000', None),
      (3, '9999999999', None),
      (4, '', None),
      (5, None, ''),
      (6, None, None),
      (7, None, 'A987654321'),
      (8, None, 'A987654321~~~B987654321'),
      (9, '1234567890', 'A987654321'),
      (10, '0000000000', 'A987654321')      
    ],
    ['id', RES_MATCHED_NHS_NO_COL, RES_MPS_ID_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, '1234567890', None, '1234567890'),
      (2, '0000000000', None, 'U'),
      (3, '9999999999', None, 'U'), 
      (4, '', None, 'U'),
      (5, None, '', 'U'),
      (6, None, None, 'U'),
      (7, None, 'A987654321', 'A987654321'),
      (8, None, 'A987654321~~~B987654321', 'A987654321'),
      (9, '1234567890', 'A987654321', '1234567890'),
      (10, '0000000000', 'A987654321', 'A987654321')      
    ],
    ['id', RES_MATCHED_NHS_NO_COL, RES_MPS_ID_COL, PERSON_ID_COL]
  )
  
  df_output = add_person_id(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
person_id_test_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # PERSON_ID_TYPE

# COMMAND ----------

add_person_id_type_test_suite = FunctionTestSuite()

# runs positive tests on expected input to validate derivation
@add_person_id_type_test_suite.add_test
def person_id_type_positive_tests():
  
  df_input = spark.createDataFrame(
    [
      (1, '5208573082'),
      (2, 'A956739783'),
      (3, 'B364280571'),
      (4, 'U964082684'),
      
    ],
    ['id', PERSON_ID_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, '5208573082', 'NHSNUMBER'),
      (2, 'A956739783', 'MPS_ID'),
      (3, 'B364280571', 'MPS_ID'),
      (4, 'U964082684', 'ONE_TIME_USE_ID'),
      
    ],
    ['id', PERSON_ID_COL, PERSON_ID_TYPE_COL]
  )
  
  df_output = add_person_id_type(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
  
# runs negative on improper input to check the derivation rules give consistent output
@add_person_id_type_test_suite.add_test
def person_id_type_negative_tests():
  
  input_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField(PERSON_ID_COL, StringType(), True)
  ])
  
  input_data = [(1, '739A123456'),
      (2, '8765U12345'),
      (3, 'QWERTYUIOP'),
      (4, ''),
      (5, None),
      (6, '520857308'),
      (7, '52085730821')      
    ]
  
  df_input = spark.createDataFrame(data=input_data,schema=input_schema)
  
  expected_output_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField(PERSON_ID_COL,StringType(),True),
    StructField(PERSON_ID_TYPE_COL,StringType(),True)
  ])

  expected_data = [(1, '739A123456', None),
      (2, '8765U12345', None),
      (3, 'QWERTYUIOP', None),
      (4, '', None),
      (5, None, None),
      (6, '520857308', None),
      (7, '52085730821', None)  
    ]
  
  df_expected = spark.createDataFrame(data= expected_data,schema=expected_output_schema)
      
  df_output = add_person_id_type(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
add_person_id_type_test_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # MPS_LAST_STEP_ATTEMPTED

# COMMAND ----------

add_mps_last_step_attempted_test_suite = FunctionTestSuite()

@add_mps_last_step_attempted_test_suite.add_test
def mps_last_step_attempted_test_no_tracing():
  '''
  Test no PDS tracing run
  '''
  df_input = spark.createDataFrame(
    [
      (1, 0, None),
      (2, 0, 0)
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, 0, None, 'No_PDS_tracing_run'),
      (2, 0, 0, 'No_PDS_tracing_run')
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, MPS_LAST_STEP_ATTEMPTED_COL]
  )
  
  df_output = add_mps_last_step_attempted(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])

  
@add_mps_last_step_attempted_test_suite.add_test
def mps_last_step_attempted_positive_tests():
  ''' 
  runs positive tests on expected input to validate derivation  
  '''
  df_input = spark.createDataFrame(
    [
      (1, 3, None),
      (2, 3, 0),
      (3, 4, None),
      (4, 4, 0),
      (5, 1, None),
      (6, 1, 0)
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, 3, None, 'alphanumeric_trace_live'),
      (2, 3, 0, 'alphanumeric_trace_live'),
      (3, 4, None, 'algorithmic_trace_live'),
      (4, 4, 0, 'algorithmic_trace_live'),
      (5, 1, None, 'CCT_cached'),
      (6, 1, 0, 'CCT_live')
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, MPS_LAST_STEP_ATTEMPTED_COL]
  )
  
  df_output = add_mps_last_step_attempted(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
  
@add_mps_last_step_attempted_test_suite.add_test
def mps_last_step_attempted_negative_tests():
  '''
  runs negative on improper input to check the derivation rules give consistent output
  '''  
  
  input_schema = StructType([      
      StructField('id', IntegerType(), True),
      StructField(RES_MATCHED_ALGORITHM_INDICATOR_COL, IntegerType(), True),
      StructField(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, IntegerType(), True)
    ])
    
  df_input = spark.createDataFrame(
      data= [
      (1, 5, None),
      (2, 1000, 1),      
      (3, 2, 0)
      ],
      schema = input_schema
    )
    
  output_schema = StructType([      
      StructField('id',IntegerType(),True),
      StructField(RES_MATCHED_ALGORITHM_INDICATOR_COL, IntegerType(), True),
      StructField(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, IntegerType(), True),
      StructField(MPS_LAST_STEP_ATTEMPTED_COL, StringType(), True)  
    ])    
    
  df_expected = spark.createDataFrame(
      data= [
      (1, 5, None, None),
      (2, 1000, 1, None),      
      (3, 2, 0, None)
      ],
      schema = output_schema
    ) 
  
  df_output = add_mps_last_step_attempted(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
add_mps_last_step_attempted_test_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # MPS_SUCCESSFUL_STEP

# COMMAND ----------

add_mps_successful_step_test_suite = FunctionTestSuite()

@add_mps_successful_step_test_suite.add_test
def test_mps_successful_step_unsuccessful_or_no_tracing():
  '''
  Test no PDS tracing run or unsuccessful
  '''
  
  df_input = spark.createDataFrame(
    [
      (1, 0, 0, None, 'ONE_TIME_USE_ID'),
      (2, 0, 0, 0, 'ONE_TIME_USE_ID'),
      (3, 3, 0, 0, 'MPS_ID'),
      (4, 4, 0, 0, 'ONE_TIME_USE_ID')
    ],
    ['id',RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, PERSON_ID_TYPE_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, 0, 0, None, 'ONE_TIME_USE_ID', 'No_PDS_tracing_run'),
      (2, 0, 0, 0, 'ONE_TIME_USE_ID', 'No_PDS_tracing_run'),
      (3, 3, 0, 0, 'MPS_ID', 'No_PDS_match_found'),
      (4, 4, 0, 0, 'ONE_TIME_USE_ID', 'No_PDS_match_found')
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, PERSON_ID_TYPE_COL, MPS_SUCCESSFUL_STEP_COL]
  )
  
  df_output = add_mps_successful_step(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])


@add_mps_successful_step_test_suite.add_test
def mps_successful_step_positive_tests():
  '''
  runs positive tests on expected input to validate derivation  
  '''
  
  df_input = spark.createDataFrame(
    [
      (1, 1, 100, 0, 'NHSNUMBER'),
      (2, 1, 100, None, 'NHSNUMBER'),
      (3, 3, 100, 0, 'NHSNUMBER'),
      (4, 3, 100, None, 'NHSNUMBER'),
      (5, 4, 50, 100, 'NHSNUMBER'),
      (6, 4, 49, 33, 'MPS_ID')
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, PERSON_ID_TYPE_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, 1, 100, 0, 'NHSNUMBER', 'CCT_live'),
      (2, 1, 100, None, 'NHSNUMBER', 'CCT_cached'),
      (3, 3, 100, 0, 'NHSNUMBER', 'alphanumeric_trace_live'),
      (4, 3, 100, None, 'NHSNUMBER', 'alphanumeric_trace_live'),
      (5, 4, 50, 100, 'NHSNUMBER', 'algorithmic_trace_live'),
      (6, 4, 49, 33, 'MPS_ID', None)
    ],
    ['id', RES_MATCHED_ALGORITHM_INDICATOR_COL, RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL,PERSON_ID_TYPE_COL, MPS_SUCCESSFUL_STEP_COL]
  )
  
  df_output = add_mps_successful_step(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])


@add_mps_successful_step_test_suite.add_test
def mps_successful_step_negative_tests():
  '''
  runs negative on improper input to check the derivation rules give consistent output
  '''
  
  input_schema = StructType([      
      StructField('id', IntegerType(), True),
      StructField(RES_MATCHED_ALGORITHM_INDICATOR_COL, IntegerType(), True),
      StructField(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, IntegerType(), True),
      StructField(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, IntegerType(), True),
      StructField(PERSON_ID_TYPE_COL, StringType(), True)
    ])  
  
  df_input = spark.createDataFrame(
    [
      (1, 5, 100, 100, None),
      (2, 1000, 1, 100, None),
      (3, 2, 50, 0, None) 
    ],
    schema = input_schema
  )
  
  output_schema = StructType([      
      StructField('id', IntegerType(), True),
      StructField(RES_MATCHED_ALGORITHM_INDICATOR_COL, IntegerType(), True),
      StructField(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, IntegerType(), True),
      StructField(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, IntegerType(), True),
      StructField(PERSON_ID_TYPE_COL, StringType(), True),
      StructField(MPS_SUCCESSFUL_STEP_COL, StringType(), True)
    ]) 
  
  df_expected = spark.createDataFrame(
    [
      (1, 5, 100, 100, None, None),
      (2, 1000, 1, 100, None, None),
      (3, 2, 50, 0, None, None)      
    ],
    schema = output_schema
  )
  
  df_output = add_mps_successful_step(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
    
    
add_mps_successful_step_test_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # PDS_MATCH_FLAG

# COMMAND ----------

add_pds_match_flag_suite = FunctionTestSuite()

@add_pds_match_flag_suite.add_test
def pds_match_flag_tests():
  '''
  Testing the cases for the PDS_MATCH_FLAG 
  '''
  
  df_input = spark.createDataFrame(
    [
      (1, 'NHSNUMBER'),
      (2, 'MPS_ID'),
      (3, 'ONE_TIME_USE_ID'),
      (4, None),
      (5, '')
    ],
    ['id', PERSON_ID_TYPE_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, 'NHSNUMBER', True),
      (2, 'MPS_ID', False),
      (3, 'ONE_TIME_USE_ID', False),
      (4, None, False),
      (5, '', False)
    ],
    ['id', PERSON_ID_TYPE_COL, PDS_MATCH_FLAG_COL]
  )
  
  df_output = add_pds_match_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
add_pds_match_flag_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDATE_SCHEMA

# COMMAND ----------

TEST_KEYS = {
  'FROM_KEY': 'from',
  'TO_KEY': 'to',
  'CODE_KEY': 'code',
  'SED_HIGH_KEY': 'sedHigh',
  'SED_LOW_KEY': 'sedLow',
  'VALUE_KEY': 'value'
}

TEST_SCHEMA = {
  'NHS_NUMBER_COL': {
    'NAME': 'nhs_number',
    'KEYS': []
  },
  'REPLACEMENT_OF_COL': {
    'NAME': 'replacementOf',
    'KEYS': drop_keys(TEST_KEYS, ['CODE_KEY'])
  },
  'CONFIDENTIALITY_CODE_COL': {
    'NAME': 'confidentialityCode',
    'KEYS': drop_keys(TEST_KEYS, ['VALUE_KEY'])
  },
  'CONFIDENTIALITY_CODE_HISTORY_COL': {
    'NAME': 'confidentialityCode_history',
    'KEYS': drop_keys(TEST_KEYS, ['VALUE_KEY'])
  }
}

validate_schema_suite = FunctionTestSuite()


@validate_schema_suite.add_test
def complete_schema():
  df_input = spark.createDataFrame(
    [],
    StructType([
      StructField(TEST_SCHEMA['NHS_NUMBER_COL']['NAME'], StringType()),
      StructField(TEST_SCHEMA['REPLACEMENT_OF_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['VALUE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_HISTORY_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      ))
    ])
  )

  df_output = validate_schema(df_input, TEST_SCHEMA)
  assert schema_correct(df_output)
  
  
@validate_schema_suite.add_test
def missing_column():
  df_input = spark.createDataFrame(
    [],
    StructType([
      StructField('nhsNumber', StringType()),
      StructField(TEST_SCHEMA['REPLACEMENT_OF_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['VALUE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_HISTORY_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      ))
    ])
  )

  df_output = validate_schema(df_input, TEST_SCHEMA)
  assert not schema_correct(df_output)
  
  
@validate_schema_suite.add_test
def missing_key():
  df_input = spark.createDataFrame(
    [],
    StructType([
      StructField(TEST_SCHEMA['NHS_NUMBER_COL']['NAME'], StringType()),
      StructField(TEST_SCHEMA['REPLACEMENT_OF_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField('sed_low', StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['VALUE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_HISTORY_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      ))
    ])
  )

  df_output = validate_schema(df_input, TEST_SCHEMA)
  assert not schema_correct(df_output)
  
  
@validate_schema_suite.add_test
def incomplete_schema():
  df_input = spark.createDataFrame(
    [],
    StructType([
      StructField(TEST_SCHEMA['NHS_NUMBER_COL']['NAME'], StringType()),
      StructField(TEST_SCHEMA['REPLACEMENT_OF_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['VALUE_KEY'], StringType())
        ])
      )),
      StructField(TEST_SCHEMA['CONFIDENTIALITY_CODE_COL']['NAME'], ArrayType(
        StructType([
          StructField(TEST_KEYS['FROM_KEY'], IntegerType()),
          StructField(TEST_KEYS['TO_KEY'], IntegerType()),
          StructField(TEST_KEYS['SED_LOW_KEY'], StringType()),
          StructField(TEST_KEYS['SED_HIGH_KEY'], StringType()),
          StructField(TEST_KEYS['CODE_KEY'], StringType())
        ])
      )),
    ])
  )

  df_output = validate_schema(df_input, TEST_SCHEMA)
  assert not schema_correct(df_output)
  
  
validate_schema_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # SUPERSEDED_NHS_NUMBER_FLAG

# COMMAND ----------

superseded_nhs_number_flag_suite = FunctionTestSuite()

@superseded_nhs_number_flag_suite.add_test
def single_match_tests():
  df_input = spark.createDataFrame(
    [
      ('1', '1111111111', datetime(2010, 7, 1), 20100701, '2222222222'),
      ('2', '1111111111', datetime(2010, 1, 1), 20100101, '2222222222'),
      ('3', '1111111111', datetime(2010, 1, 2), 20100102, '2222222222'),
      ('4', '1111111111', datetime(2020, 1, 1), 20200101, '2222222222'),
      ('5', '1111111111', datetime(2020, 1, 2), 20200102, '2222222222'),
      ('6', '3333333333', datetime(2010, 7, 1), 20100701, '4444444444'),
      ('7', '1111111111', datetime(2009, 7, 1), 20090701, '2222222222'),
      ('8', '1111111111', datetime(2021, 7, 1), 20210701, '2222222222'),
      ('9', '1111111111', datetime(2010, 7, 1), 20100701, '5555555555'),
      ('10', '5555555555', datetime(2010, 7, 1), 20100701, '2222222222'),
      ('11', 'ABCD111111', datetime(2010, 7, 1), 20100701, '6666666666'),
      ('12', '1111111111', datetime(2010, 7, 1), None, '2222222222'),
      ('13', '7777777777', datetime(2010, 7, 1), 20100701, '5555555555'),
      ('14', '8888888888', datetime(2010, 7, 1), 20100701, '9999999999')
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_NHS_NO_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType())
    ])
  )
  
  df_pds_full = spark.createDataFrame(
    [
      ('1111111111', None),
      ('2222222222', [{'from': 20100101, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '1111111111'}]),
      ('3333333333', None),
      ('4444444444', [{'from': 20100101, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '3333333333'}]),
      ('5555555555', None),
      ('6666666666', [{'from': None, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': 'ABCD111111'}]),
      ('8888888888', None),
      ('9999999999', [{'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '8888888888'}]),
    ],
    StructType([
      StructField('nhs_number', StringType()),
      StructField('replacementOf', ArrayType(
        StructType([
          StructField('from', IntegerType()),
          StructField('to', IntegerType()),
          StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
          StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
          StructField('value', StringType())
        ])
      ))
    ])
  )
  
  df_expected = spark.createDataFrame(
    [
      ('1', '1111111111', datetime(2010, 7, 1), 20100701, '2222222222', True), # happy path: both nhs numbers match
      ('2', '1111111111', datetime(2010, 1, 1), 20100101, '2222222222', False), # both nhs numbers match but request date is on the replacement of sedLow date
      ('3', '1111111111', datetime(2010, 1, 2), 20100102, '2222222222', True), # both nhs numbers match and request date is the day after the replacement of sedLow date
      ('4', '1111111111', datetime(2020, 1, 1), 20200101, '2222222222', True), # both nhs numbers match and request date is on the replacement of sedHigh date
      ('5', '1111111111', datetime(2020, 1, 2), 20200102, '2222222222', False), # both nhs numbers match but request date is the day after the replacement of sedHigh date
      ('6', '3333333333', datetime(2010, 7, 1), 20100701, '4444444444', True), # happy path: both nhs numbers match and replacement of sedHigh date is null
      ('7', '1111111111', datetime(2009, 7, 1), 20090701, '2222222222', False), # nhs numbers both match but request date is before replacement of sedLow date
      ('8', '1111111111', datetime(2021, 7, 1), 20210701, '2222222222', False), # nhs numbers both match but request date is after replacement of sedHigh date
      ('9', '1111111111', datetime(2010, 7, 1), 20100701, '5555555555', False), # response nhs number doesn't match the PDS record
      ('10', '5555555555', datetime(2010, 7, 1), 20100701, '2222222222', False), # request nhs number doesn't match the PDS replacement of value
      ('11', 'ABCD111111', datetime(2010, 7, 1), 20100701, '6666666666', False), # both nhs numbers match but the replacement of NHS number is in an old format
      ('12', '1111111111', datetime(2010, 7, 1), None, '2222222222', True), # request AS_AT_date is null so uses created date instead
      ('13', '7777777777', datetime(2010, 7, 1), 20100701, '5555555555', False), # response nhs number is in PDS but without anything in replacementOf
      ('14', '8888888888', datetime(2010, 7, 1), 20100701, '9999999999', True) # when sedLow is null, the request date is considered to be in range
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_NHS_NO_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType()),
       StructField(SUPERSEDED_NHS_NUMBER_FLAG_COL, BooleanType())
 
    ])
  )
  
  df_output = add_superseded_nhs_number_flag(df_input, df_pds_full)
  assert compare_results(df_output, df_expected, join_columns = [UNIQUE_REFERENCE_COL]) 
  

@superseded_nhs_number_flag_suite.add_test
def multiple_match_tests():
  df_input = spark.createDataFrame(
    [
      ('1', '1111111111', datetime(2010, 7, 1), 20100701, '2222222222'),
      ('2', '3333333333', datetime(2010, 7, 1), 20100701, '4444444444'), 
      ('3', '6666666666', datetime(2010, 7, 1), 20100701, '7777777777'),
      ('4', '8888888888', datetime(2010, 7, 1), 20100701, '9999999999'),
      ('5', '1212121212', datetime(2010, 7, 1), 20100701, '1313131313'),
      ('6', '1515151515', datetime(2010, 7, 1), 20100701, '1616161616')
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_NHS_NO_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType())
    ])
  )
  
  df_pds_full = spark.createDataFrame(
    [
      ('2222222222', [
        {'from': 20100101, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '1111111111'},
        {'from': 20090101, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: None, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '1111111111'}
      ]),
      ('4444444444', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '3333333333'},
        {'from': None, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: 20120101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '3333333333'}
      ]),
      ('7777777777', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '6666666666'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '5555555555'}
      ]),
      ('9999999999', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20120101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '8888888888'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20150101090000, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '8888888888'}
      ]),
      ('1313131313', [
        {'from': 20150101, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: 20150101090000, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '1212121212'}
      ]),
      ('1313131313', [
        {'from': 20100101, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: None, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '1414141414'}
      ]),
      ('1616161616', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '1515151515'}
      ]),
      ('1616161616', [
        {'from': 20090101, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, PDS_KEYS['SED_HIGH_KEY']: 20200101090000, 'value': '1515151515'}
      ]),
      ('1616161616', [
        {'from': None, 'to': 20200101, PDS_KEYS['SED_LOW_KEY']: 20080101090000, PDS_KEYS['SED_HIGH_KEY']: None, 'value': '1515151515'}
      ])
    ],
    StructType([
      StructField('nhs_number', StringType()),
      StructField('replacementOf', ArrayType(
        StructType([
          StructField('from', IntegerType()),
          StructField('to', IntegerType()),
          StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
          StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
          StructField('value', StringType())
        ])
      ))
    ])
  )
  
  df_expected = spark.createDataFrame(
    [
      ('1', '1111111111', datetime(2010, 7, 1), 20100701, '2222222222', True), # multiple matches (in replacement of col) on nhs number with all dates matching, only one True flag is kept
      ('2', '3333333333', datetime(2010, 7, 1), 20100701, '4444444444', True), # multiple matches (in replacement of col) on nhs number with some dates matching, only one True flag is kept
      ('3', '6666666666', datetime(2010, 7, 1), 20100701, '7777777777', True), # multiple matches on dates but only one match on replacement of nhs number, only one True flag is kept
      ('4', '8888888888', datetime(2010, 7, 1), 20100701, '9999999999', False), # multiple matches (in replacement of col) on nhs number with no dates matching. False flag is kept
      ('5', '1212121212', datetime(2010, 7, 1), 20100701, '1313131313', False), # multiple matches (in nhs_number col), only one flag is kept
      ('6', '1515151515', datetime(2010, 7, 1), 20100701, '1616161616', True) # count of returned records is the same as count of input records, even if multiple matches in the left join
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_NHS_NO_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType()),
       StructField(SUPERSEDED_NHS_NUMBER_FLAG_COL, BooleanType())
 
    ])
  )
  
  df_output = add_superseded_nhs_number_flag(df_input, df_pds_full)
  assert compare_results(df_output, df_expected, join_columns = [UNIQUE_REFERENCE_COL]) 
  
superseded_nhs_number_flag_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # NHS_NUMBER_HISTORY_LIST

# COMMAND ----------

nhs_number_history_list_suite = FunctionTestSuite()

@nhs_number_history_list_suite.add_test
def nhs_number_history_list_tests():
  df_input = spark.createDataFrame(
    [
      ('1', datetime(2010, 7, 1), 20100701, '4444444444'),
      ('2', datetime(2010, 7, 1), 20100701, '8888888888'),
      ('3', datetime(2013, 1, 1), None, '8888888888'),
      ('4', datetime(2010, 7, 1), 20100701, '1010101010'),
      ('5', datetime(2010, 7, 1), 20100701, '1212121212'),
      ('6', datetime(2010, 7, 1), 20100701, '1313131313'),
      ('7', datetime(2010, 7, 1), 20100701, '1717171717'),
      ('8', datetime(2010, 7, 1), 20100701, '1919191919')
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType())
    ])
  )
  
  df_pds_full = spark.createDataFrame(
    [
      ('4444444444', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, 'value': '1111111111'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20090101090000, 'value': '2222222222'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20080101090000, 'value': '3333333333'}
      ]),
      ('8888888888', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20150101090000, 'value': '7777777777'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20090101090000, 'value': '5555555555'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20120101090000, 'value': '6666666666'}
      ]),
      ('1010101010', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, 'value': '9999999999'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20090101090000, 'value': '9999999999'}
      ]),
      ('1212121212', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, 'value': 'ABCD111111'},
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20090101090000, 'value': 'ABCD222222'}
      ]),
      ('1717171717', [
        {'from': 20100101, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, 'value': '1414141414'},
        {'from': 20090101, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, 'value': '1515151515'}
      ]),
      ('1717171717', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20080101090000, 'value': '1616161616'}
      ]),
      ('1919191919', [
        {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, 'value': '1818181818'}
      ])
    ],
    StructType([
      StructField('nhs_number', StringType()),
      StructField('replacementOf', ArrayType(
        StructType([
          StructField('from', IntegerType()),
          StructField('to', IntegerType()),
          StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
          StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
          StructField('value', StringType())
        ])
      ))
    ])
  )
  
  df_expected = spark.createDataFrame(
    [
      ('1', datetime(2010, 7, 1), 20100701, '4444444444', ['3333333333', '2222222222', '1111111111']), # happy path. Three NHS numbers are listed, in date order.
      ('2', datetime(2010, 7, 1), 20100701, '8888888888', ['5555555555']), # replacement ofs which don't match the dates are not in the list.
      ('3', datetime(2013, 1, 1), None, '8888888888', ['5555555555', '6666666666']), # if the AS_AT_date is null then revert to the req created date.
      ('4', datetime(2010, 7, 1), 20100701, '1010101010', ['9999999999']), # where there are duplicates in replacement of, they are only listed once.
      ('5', datetime(2010, 7, 1), 20100701, '1212121212', []), # replacements of old nhs numbers (ABCD1234) are not listed.
      ('6', datetime(2010, 7, 1), 20100701, '1313131313', []), # if the response NHS number is not in PDS, then the list is empty.
      ('7', datetime(2010, 7, 1), 20100701, '1717171717', ['1616161616', '1515151515', '1414141414']), # where duplicate replacement ofs across multiple PDS rows, they are all listed.
      ('8', datetime(2010, 7, 1), 20100701, '1919191919', ['1818181818']) # if the sedLow is null, the request date is considered to be later than it.
    ],
    StructType([
      StructField(UNIQUE_REFERENCE_COL, StringType()),
      StructField(REQ_CREATED_COL, TimestampType()),
      StructField(REQ_AS_AT_DATE_COL, IntegerType()),
      StructField(RES_MATCHED_NHS_NO_COL, StringType()),
      StructField(NHS_NUMBER_HISTORY_LIST_COL, ArrayType(StringType(), False), True)
    ])
  )
  
  df_output = add_nhs_number_history_list(df_input, df_pds_full, excludeSensitiveRecords=False)
  assert compare_results(df_output, df_expected, join_columns = [UNIQUE_REFERENCE_COL], allow_nullable_schema_mismatch = True) # allow_nullable_schema_mismatch ensures backwards compatibility

  
@nhs_number_history_list_suite.add_test
def nhs_number_history_list_sensitive_tests():
  df_input = spark.createDataFrame(
    [
      ('1', datetime(2010, 7, 1), 20100701, '4444444444', False),
      ('2', datetime(2010, 7, 1), 20100701, '4444444444', True)
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType()),
       StructField(SENSITIVE_FLAG_COL, BooleanType())
    ])
  )

  df_pds_full = spark.createDataFrame(
    [
      ('4444444444', [{'from': 20100101, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, 'value': '1111111111'}, 
                      {'from': 20090101, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, 'value': '2222222222'}, 
                      {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20080101090000, 'value': '3333333333'}],
                     [{'code': 'I'}],       
                     [{'code': 'I'}]),
      ('1111111111', [], [{'code': 'I'}], [{'code': 'I'}]),
      ('2222222222', [], [{'code': 'I'}], [{'code': 'I'}, {'code': 'S'}]),
      ('3333333333', [], [{'code': 'S'}], [{'code': 'S'}]),
    ],
    StructType([
      StructField('nhs_number', StringType()),
      StructField('replacementOf', ArrayType(
        StructType([
          StructField('from', IntegerType()),
          StructField('to', IntegerType()),
          StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
          StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
          StructField('value', StringType())
        ])
      )),
      StructField('confidentialityCode', ArrayType(StructType([StructField('code', StringType())]))),
      StructField('confidentialityCode_history', ArrayType(StructType([StructField('code', StringType())])))
    ])
  )

  df_expected = spark.createDataFrame(
    [
      ('1', datetime(2010, 7, 1), 20100701, '4444444444', False, ['1111111111']), # '2222222222' and '3333333333' are not listed because they are or were sensitive
      ('2', datetime(2010, 7, 1), 20100701, '4444444444', True, []) # no history is returned because the record itself was sensitive

    ],
    StructType([
      StructField(UNIQUE_REFERENCE_COL, StringType()),
      StructField(REQ_CREATED_COL, TimestampType()),
      StructField(REQ_AS_AT_DATE_COL, IntegerType()),
      StructField(RES_MATCHED_NHS_NO_COL, StringType()),
      StructField(SENSITIVE_FLAG_COL, BooleanType()),
      StructField(NHS_NUMBER_HISTORY_LIST_COL, ArrayType(StringType(), True), True)
    ])
  )

  df_output = add_nhs_number_history_list(df_input, df_pds_full, excludeSensitiveRecords=True)
  assert compare_results(df_output, df_expected, join_columns = [UNIQUE_REFERENCE_COL], allow_nullable_schema_mismatch = True) # allow_nullable_schema_mismatch flag ensures backwards compatibility 

  
@nhs_number_history_list_suite.add_test
def nhs_number_history_list_sensitive_flag_off_tests():
  df_input = spark.createDataFrame(
    [
      ('1', datetime(2010, 7, 1), 20100701, '4444444444', False),
      ('2', datetime(2010, 7, 1), 20100701, '4444444444', True)
    ],
    StructType([
       StructField(UNIQUE_REFERENCE_COL, StringType()),
       StructField(REQ_CREATED_COL, TimestampType()),
       StructField(REQ_AS_AT_DATE_COL, IntegerType()),
       StructField(RES_MATCHED_NHS_NO_COL, StringType()),
       StructField(SENSITIVE_FLAG_COL, BooleanType())
    ])
  )

  df_pds_full = spark.createDataFrame(
    [
      ('4444444444', [{'from': 20100101, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20100101090000, 'value': '1111111111'}, 
                      {'from': 20090101, 'to': None, PDS_KEYS['SED_LOW_KEY']: None, 'value': '2222222222'}, 
                      {'from': None, 'to': None, PDS_KEYS['SED_LOW_KEY']: 20080101090000, 'value': '3333333333'}],
                     [{'code': 'I'}],       
                     [{'code': 'I'}]),
      ('1111111111', [], [{'code': 'I'}], [{'code': 'I'}]),
      ('2222222222', [], [{'code': 'I'}], [{'code': 'I'}, {'code': 'S'}]),
      ('3333333333', [], [{'code': 'S'}], [{'code': 'S'}]),
    ],
    StructType([
      StructField('nhs_number', StringType()),
      StructField('replacementOf', ArrayType(
        StructType([
          StructField('from', IntegerType()),
          StructField('to', IntegerType()),
          StructField(PDS_KEYS['SED_LOW_KEY'], StringType()),
          StructField(PDS_KEYS['SED_HIGH_KEY'], StringType()),
          StructField('value', StringType())
        ])
      )),
      StructField('confidentialityCode', ArrayType(StructType([StructField('code', StringType())]))),
      StructField('confidentialityCode_history', ArrayType(StructType([StructField('code', StringType())])))
    ])
  )

  df_expected = spark.createDataFrame(
    [
      ('1', datetime(2010, 7, 1), 20100701, '4444444444', False, ['3333333333', '2222222222', '1111111111']), # '2222222222' and '3333333333' are listed even though they are senstive because excludeSensitiveRecords=False
      ('2', datetime(2010, 7, 1), 20100701, '4444444444', True, ['3333333333', '2222222222', '1111111111']) # history is returned even though the record is senstive because excludeSensitiveRecords=False

    ],
    StructType([
      StructField(UNIQUE_REFERENCE_COL, StringType()),
      StructField(REQ_CREATED_COL, TimestampType()),
      StructField(REQ_AS_AT_DATE_COL, IntegerType()),
      StructField(RES_MATCHED_NHS_NO_COL, StringType()),
      StructField(SENSITIVE_FLAG_COL, BooleanType()),
      StructField(NHS_NUMBER_HISTORY_LIST_COL, ArrayType(StringType(), False), True)
    ])
  )

  df_output = add_nhs_number_history_list(df_input, df_pds_full, excludeSensitiveRecords=False)
  assert compare_results(df_output, df_expected, join_columns = [UNIQUE_REFERENCE_COL], allow_nullable_schema_mismatch = True) # allow_nullable_schema_mismatch ensures backwards compatibility
  
nhs_number_history_list_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # SENSITIVE_FLAG

# COMMAND ----------

sensitive_flag_suite = FunctionTestSuite()

@sensitive_flag_suite.add_test
def all_sensitive_flag_tests():
  df_input = spark.createDataFrame(
    [
      (1, 'S'),
      (2, 'I'),
      (3, 'Y'),
      (4, 'N'),
      (5, 'B'),
      (6, None)
    ],
    StructType([
        StructField('id', IntegerType()),
        StructField(RES_SENSITIVITY_FLAG_COL, StringType()),
    ])
  )

  df_expected = spark.createDataFrame(
    [
      (1, 'S', True),
      (2, 'I', True),
      (3, 'Y', True),
      (4, 'N', False),
      (5, 'B', False),
      (6, None, False)
    ],
    StructType([
        StructField('id', IntegerType()),
        StructField(RES_SENSITIVITY_FLAG_COL, StringType()),
        StructField(SENSITIVE_FLAG_COL, BooleanType())
    ])
  )

  df_output = add_sensitive_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
  
sensitive_flag_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # MULTIPLE_PDS_MATCHES_FLAG

# COMMAND ----------

multiple_pds_matches_flag_suite = FunctionTestSuite()

 
@multiple_pds_matches_flag_suite.add_test
def test_multiple_pds_matches():
  df_input = spark.createDataFrame(
    [
      (1, '97', '9999999999', 'U123456789'),
      (2, '', '123456789', 'U123456789'),
      (3, None, '9999999999', 'U123456789'),
      (4, '99', '9999999999', None)
    ],
    ['id', RES_ERROR_SUCCESS_CODE_COL, RES_MATCHED_NHS_NO_COL, PERSON_ID_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, '97', '9999999999', 'U123456789', True),
      (2, '', '123456789', 'U123456789', False),
      (3, None, '9999999999', 'U123456789', False),
      (4, '99', '9999999999', None, False)
    ],
    ['id', RES_ERROR_SUCCESS_CODE_COL, RES_MATCHED_NHS_NO_COL, PERSON_ID_COL, MULTIPLE_PDS_MATCHES_FLAG_COL]
  )
  
  df_output = add_multiple_pds_matches_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
  
multiple_pds_matches_flag_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # MULTIPLE_MPS_ID_MATCHES_FLAG

# COMMAND ----------

multiple_mps_id_matches_flag_suite = FunctionTestSuite()


@multiple_mps_id_matches_flag_suite.add_test
def test_mupltiple_mps_id_matches():
  df_input = spark.createDataFrame(
    [
      (1, 'A100~~~B100'),
      (2, 'A100'),
      (3, ''),
      (4, None),
      (5, 'A100, B200'),
      (6, 'A100~B200')
    ],
    ['id', RES_MPS_ID_COL]
  )

  df_expected = spark.createDataFrame(
    [
      (1, 'A100~~~B100', True),
      (2, 'A100', False),
      (3, '', False),
      (4, None, False),
      (5, 'A100, B200', False),
      (6, 'A100~B200', False)
    ],
    ['id', RES_MPS_ID_COL, MULTIPLE_MPS_ID_MATCHES_FLAG_COL]
  )
  
  df_output = add_multiple_mps_id_matches_flag(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
  
multiple_mps_id_matches_flag_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC #MPS_MATCH_SCORE

# COMMAND ----------

mps_match_score_suite = FunctionTestSuite()


@mps_match_score_suite.add_test
def test_mps_match_score():
  
  df_input = spark.createDataFrame(
    [
      (1, Decimal(0.00)),
      (2, Decimal(33.33)),
      (3, Decimal(50.00)),
      (4, Decimal(66.66)),
      (5, Decimal(100.00)),
      (6, None)
    ],
    StructType([
      StructField('id', IntegerType(), False), 
      StructField(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, DecimalType(5,2), True)
    ])
  )

  df_expected = spark.createDataFrame(
    [
      (1, Decimal(0.00), Decimal(0.00)),
      (2, Decimal(33.33), Decimal(33.33)),
      (3, Decimal(50.00), Decimal(50.00)),
      (4, Decimal(66.66), Decimal(66.66)),
      (5, Decimal(100.00), Decimal(100.00)),
      (6, None, None)
    ],
    StructType([
      StructField('id', IntegerType(), False), 
      StructField(RES_MATCHED_CONFIDENCE_PERCENTAGE_COL, DecimalType(5,2), True), 
      StructField(MPS_MATCH_SCORE_COL, DecimalType(5,2), True)
    ])
  )
  
  df_output = add_mps_match_score(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  

mps_match_score_suite.run()

# COMMAND ----------

# MAGIC %md
# MAGIC # MPS_ALGORITHMIC_MATCH_SCORE

# COMMAND ----------

mps_algorithmic_match_score_suite = FunctionTestSuite()


@mps_algorithmic_match_score_suite.add_test
def test_mps_algorithmic_match_score():
  
  df_input = spark.createDataFrame(
    [
      (1, Decimal(10), Decimal(20), Decimal(30), Decimal(40), Decimal(50.55)),
      (2, None, None, None, Decimal(0), Decimal(0))
    ],
    StructType([
        StructField('id', IntegerType(), False),
        StructField(RES_ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_GENDER_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC_COL, DecimalType(5,2), True)
      ])
  )

  df_expected = spark.createDataFrame(
    [
      (1, Decimal(10), Decimal(20), Decimal(30), Decimal(40), Decimal(50.55), (Decimal(10), Decimal(20), Decimal(30), Decimal(40), Decimal(50.55))),
      (2, None, None, None, Decimal(0), Decimal(0), (None, None, None, Decimal(0), Decimal(0)))
    ],
    StructType([
        StructField('id', IntegerType(), False),
        StructField(RES_ALGORITHMIC_TRACE_FAMILY_NAME_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_GIVEN_NAME_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_DOB_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_GENDER_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(RES_ALGORITHMIC_TRACE_POSTCODE_SCORE_PERC_COL, DecimalType(5,2), True),
        StructField(MPS_ALGORITHMIC_MATCH_SCORE_COL, 
                    StructType([
                      StructField(FAMILYNAME_COL, DecimalType(5,2), True),
                      StructField(GIVENNAME_COL, DecimalType(5,2), True),
                      StructField(DATEOFBIRTH_COL, DecimalType(5,2), True),
                      StructField(GENDER_COL, DecimalType(5,2), True),
                      StructField(POSTCODE_COL, DecimalType(5,2), True)
                    ]))
      ])
  )
  
  df_output = add_mps_algorithmic_match_score(df_input)
  assert compare_results(df_output, df_expected, join_columns = ['id'])
  
  
mps_algorithmic_match_score_suite.run()