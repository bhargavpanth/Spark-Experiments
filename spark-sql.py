from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('test_spark_sql').getOrCreate()
