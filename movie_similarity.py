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

def movie_similarity(movie_pairs):
    # Compute xx, xy and yy columns
    pair_wise_scores = movie_pairs \
        .withColumn('xx', func.col('rating1') * func.col('rating1')) \
        .withColumn('yy', func.col('rating2') * func.col('rating2')) \
        .withColumn('xy', func.col('rating1') * func.col('rating2'))
    calculate_similarity = pair_wise_scores \
        .groupBy('movie1', 'movie2') \
        .agg( \
            func.sum(func.col('xy')).alias('numerator'), \
            (func.sqrt(func.sum(func.col('xx'))) * func.sqrt(func.sum(func.col('yy')))).alias('denominator'), \
            func.count(func.col('xy')).alias('num_pairs')
        )
    # Calculate score and select only needed columns (movie1, movie2, score, num_pairs)
    result = calculate_similarity \
        .withColumn('score', \
            func.when(func.col('denominator') != 0, func.col('numerator') / func.col('denominator')) \
            .otherwise(0) \
        ).select('movie1', 'movie2', 'score', 'num_pairs')
    return result

def main():
    name_schema = movie_name_schema()
    # Broadcast dataset of movieID and movieTitle
    movie_names = spark.read.option('sep', '|').option('charset', 'ISO-8859-1') \
      .schema(name_schema).csv('./ml-100k/u.item')
    # Movie data
    schema = movie_schema()
    movies = spark.read.option('sep', '\t').schema(schema) \
      .csv('./ml-100k/u.data')
    # Ratings
    ratings = movies.select('userId', 'movieId', 'rating')
    movie_pairs = ratings.alias('ratings1') \
      .join(ratings.alias('ratings2'), (func.col('ratings1.userId') == func.col('ratings2.userId')) \
            & (func.col('ratings1.movieId') < func.col('ratings2.movieId'))) \
      .select(func.col('ratings1.movieId').alias('movie1'), \
        func.col('ratings2.movieId').alias('movie2'), \
        func.col('ratings1.rating').alias('rating1'), \
        func.col('ratings2.rating').alias('rating2'))
    # Compute the cosine similarity between the movies
    pairs = movie_similarity(movie_pairs)
