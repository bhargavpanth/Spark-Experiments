from pyspark.sql import SparkSession
from pyspark.sql import functions as func

class WordCount:
    def __init__(self):
        self.spark = SparkSession.builder.appName('WordCount').getOrCreate()
        self.inputDF = self.spark.read.text('./datasets/modern_prometheus.txt')
