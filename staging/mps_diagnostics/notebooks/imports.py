# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, IntegerType, DecimalType, ArrayType, MapType, BooleanType
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from nhsdccl import util
from typing import List
from datetime import datetime
from decimal import Decimal