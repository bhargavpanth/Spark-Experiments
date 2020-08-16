from pyspark.sql import SparkSession
from pyspark.sql import Row

class SparkSession:
    def __init__(self):
        self.spark = SparkSession.builder.appName('test_spark_sql').getOrCreate()
