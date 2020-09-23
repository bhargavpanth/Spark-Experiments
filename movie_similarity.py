'''
Script uses Spark's cluster manager
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('movie_similarities').master('local[*]').getOrCreate()


def movie_name_schema():
    return StructType([ \
        StructField('movieID', IntegerType(), True), \
        StructField('movieTitle', StringType(), True) \
    ])

def movie_schema():
    return StructType([ \
                StructField('userID', IntegerType(), True), \
                StructField('movieID', IntegerType(), True), \
                StructField('rating', IntegerType(), True), \
                StructField('timestamp', LongType(), True) \
            ])

def main():
    name_schema = movie_name_schema()
    # Broadcast dataset of movieID and movieTitle
    movieNames = spark.read.option('sep', '|').option('charset', 'ISO-8859-1') \
      .schema(name_schema).csv('./ml-100k/u.item')