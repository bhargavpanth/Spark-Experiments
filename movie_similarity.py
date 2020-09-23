'''
Script uses Spark's cluster manager
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('movie_similarities').master('local[*]').getOrCreate()


def movie_name_schema():
    return StructType([ StructField('movieID', IntegerType(), True), StructField('movieTitle', StringType(), True) ])

def movie_schema():
    pass