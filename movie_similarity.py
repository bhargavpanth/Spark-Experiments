'''
Script uses Spark's cluster manager
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('movie_similarities').master('local[*]').getOrCreate()
