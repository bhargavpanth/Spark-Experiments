'''
Script uses Spark's cluster manager
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import functions as func

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
    # Movie data
    schema = movie_schema()
    movies = spark.read.option('sep', '\t').schema(schema) \
      .csv('./ml-100k/u.data')
    # Ratings
    ratings = movies.select('userId', 'movieId', 'rating')

    moviePairs = ratings.alias('ratings1') \
      .join(ratings.alias('ratings2'), (func.col('ratings1.userId') == func.col('ratings2.userId')) \
            & (func.col('ratings1.movieId') < func.col('ratings2.movieId'))) \
      .select(func.col('ratings1.movieId').alias('movie1'), \
        func.col('ratings2.movieId').alias('movie2'), \
        func.col('ratings1.rating').alias('rating1'), \
        func.col('ratings2.rating').alias('rating2'))

    