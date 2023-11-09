# Databricks notebook source
# MAGIC %md
# MAGIC This notebook contains copies of functions that can be found in the NHS Digital Common Code Library (NHSDCCL).
# MAGIC 
# MAGIC We also import util from nhsdccl, but in these cases a small amendment is needed for our particular purposes.

# COMMAND ----------

import unittest
from typing import *
from functools import wraps
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from nhsdccl import util

# COMMAND ----------

def clear_cache():
  spark.sql('CLEAR CACHE')

# COMMAND ----------

class FunctionTestSuite(object):
  """
  Defines class for registering/running tests.
  
  Example usage:
  
  >> suite = FunctionTestSuite()
  >>
  >> def foo():
  >>   ...
  >>
  >> @suite.add_test
  >> def test_foo():
  >>   ...
  >>
  >> suite.run()
  
  """
  def __init__(self):
    self._suite = unittest.TestSuite()
    self._runner = unittest.TextTestRunner()
    
  def add_test(self, test_func: Callable[[None], bool]) -> None:
    """ Add a test function to the suite.
    
    Example:
    >> def foo():
    >>   ...
    >>
    >> @suite.add_test
    >> def test_foo():
    >>   ...
    >>
    """
    
    @wraps(test_func)
    def clean_up_func():
      result = test_func()
      clear_cache()
      return result
    
    test_case = unittest.FunctionTestCase(clean_up_func)
    self._suite.addTest(test_case)
    
  def run(self) -> unittest.TestResult:
    """ Run the tests & print the output to the console.
    
    This method can be called once: further tests will need
    to be assigned to a new object instance.
    
    Returns:
      unittest.TestResult:
    """
    if not self._runner.run(self._suite).wasSuccessful():
      raise AssertionError()
    

# COMMAND ----------

def flatten_struct_columns(nested_df):
  """
  check_content_match doesn't work on struct columns.
  So we need to split them up into columns before we can compare the dataframes.
  """
  stack = [((), nested_df)]
  columns = []
  while len(stack) > 0:
    parents, df = stack.pop()
    for column_name, column_type in df.dtypes:
      if column_type[:6] == "struct":
        projected_df = df.select(column_name + ".*")
        stack.append((parents + (column_name,), projected_df))
      else:
        columns.append(F.col(".".join(parents + (column_name,))).alias("_".join(parents + (column_name,))))
  return nested_df.select(columns)

# COMMAND ----------

def check_schemas_match(df1: DataFrame,
                        df2: DataFrame,
                        allow_nullable_schema_mismatch=False
                       ) -> bool:
  """
  Returns True if the dataframe schemas match, or False otherwise.
  
  If allow_nullable_schema_mismatch is False then the nullability of the columns must also match.
  If True, nullability isn't included in the check.
  
  This differs from NHSDCCL. 
  In line 30, comparison must be between typeNames because the dataType of arrays specifices nullability
  (so to ignore nullability, we have to disregard this info and just focus on typeName).
  N.B. This means that the elementType of arrays is also disregarded.
  """
  
  if df1.schema == df2.schema:
    return True
  elif not allow_nullable_schema_mismatch:
    print('nullable schema mismatch not allowed, mismatch potentially present')
    return False
  
  if len(df1.schema) != len(df2.schema):
    print('schema length mismatch')    
    return False
  
  for field_1, field_2 in zip(df1.schema, df2.schema):
    if field_1.name != field_2.name:
      print('name error', field_1, field_2)      
      return False
    if field_1.dataType.typeName() != field_2.dataType.typeName():
      print('datatype error', field_1.dataType.typeName(), field_2.dataType.typeName())
      return False
    
  return True 

# COMMAND ----------

def check_content_match(df1: DataFrame,
                         df2: DataFrame,
                         join_col: List[str]
                        ) -> bool:
  
  """
  Compares the values in the common columns only.
  An outer join on the given join_cols is used to decide which records to compare.
  """
  join_condition = [df1[c].eqNullSafe(df2[c]) for c in join_col]
  df3 = df1.alias("d1").join(df2.alias("d2"), join_condition, "outer")
  df3.show()
  if df1.count() == df2.count():
    for name in set(df1.columns).intersection(set(df2.columns)):
      df3 = df3.withColumn(name + "_diff", F.when((F.col("d1." + name).isNull() & F.col("d2." + name).isNotNull()) |
                                                  (F.col("d1." + name).isNotNull() & F.col("d2." + name).isNull()), 1) \
                                            .when(F.col("d1." + name) != F.col("d2." + name), 1) \
                                            .otherwise(0))
    col_diff = [_col for _col in df3.columns if '_diff' in _col]
    diff_sum = df3.select(col_diff).groupBy().sum().first()
    
    df4 = df3.select(col_diff).groupBy().sum()
    mismatches_by_col_dict=df4.collect()[0].asDict()
    #print(mismatches_by_col_dict)
    for key, value in mismatches_by_col_dict.items():
      key_formatted = key.replace('sum(', '').replace(")", "")
      if value!=0:
        print("Content does not match in column", key_formatted)
    
    if sum(diff_sum) == 0:
      res = True
    else:
      res = False
      print('Content not match.', diff_sum)
  else:
    res = False
    print('Content not match.')
  return res

# COMMAND ----------

def compare_results(df1: DataFrame,
                    df2: DataFrame,
                    join_columns: List,
                    allow_nullable_schema_mismatch=True
                   ) -> bool:
  """
  Compare two dataframes. Used in testing to check outputs match expected outputs.
  """
  df1 = flatten_struct_columns(df1)
  df2 = flatten_struct_columns(df2)
  
  if check_schemas_match(df1, df2, allow_nullable_schema_mismatch) is True:
    print('Schema match.')
    if check_content_match(df1, df2, join_columns) is True:
      print('Content match.')
      return True
    else:
      print('Content mismatch.')
      return False
  else:
    print('Schema mismatch.')
    return False

# COMMAND ----------

def create_partitioned_table(spark: SparkSession, df: DataFrame, partition_by: str, db_or_asset: str,
                            table: str = None,
                            overwrite: bool = False,
                            owner: str = None):
  """
  This differs from the NHSDCCL function create_table, in that it also partitions the table.
  """
  if table is None:
    asset_name = db_or_asset
  else:
    asset_name = f'{db_or_asset}.{table}'
  
  if overwrite:
    df.write.partitionBy(partition_by).saveAsTable(asset_name, mode='overwrite')
  else:
    df.write.partitionBy(partition_by).saveAsTable(asset_name)
    
  if owner:
    spark.sql(f'ALTER TABLE {asset_name} OWNER TO `{owner}`')

# COMMAND ----------

def create_partitioned_table_from_schema(spark, partition_by: str, schema: StructType, db_or_asset: str, table: str = None, overwrite: bool = False, 
                                        allow_nullable_schema_mismatch: bool = False):
  """
  This differs from the NHSDCCL function create_table_from_schema, in that it also partitions the table.
  """
  
  if table is None:
    db = db_or_asset.split('.')[0]
    table = db_or_asset.split('.')[1]
    asset_name = db_or_asset
  else:
    db = db_or_asset
    asset_name = f'{db}.{table}'
    
  df_new = spark.createDataFrame([], schema)
  
  if util.table_exists(spark, db, table):
    if overwrite:
      util.drop_table(spark, db, table)
      create_partitioned_table(spark, df_new, partition_by, db, table)
    else:
      df_existing = spark.table(asset_name)
      if check_schemas_match(df_existing, df_new,
                             allow_nullable_schema_mismatch=allow_nullable_schema_mismatch) is False:
        raise AssertionError(f'The given new schema does not match the existing schema, and overwrite is set '
                             f'to False.\nExisting: {df_existing.schema.json()}\nNew     : '
                             f'{df_new.schema.json()}')
  else:
    create_partitioned_table(spark, df_new, partition_by, db, table)