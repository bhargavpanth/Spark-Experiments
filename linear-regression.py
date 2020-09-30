from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('linear_regression').getOrCreate()