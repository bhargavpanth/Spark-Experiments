from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class Graph:
    def __init__(self):
        self.spark = SparkSession.builder.appName('degree_of_seperation').getOrCreate()
        self.schema = StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True)])

    def read(self):
        schema = self.schema
        names = self.spark.read.schema(schema).option('sep', ' ').csv('./datasets/names.txt')
        lines = self.spark.read.text('./datasets/graph.txt')
        return [names, lines]

    def connections(self):
        [names, lines] = self.read()
        connections = lines.withColumn('id', func.split(func.col('value'), ' ')[0]) \
            .withColumn('connections', func.size(func.split(func.col('value'), ' ')) - 1) \
            .groupBy('id').agg(func.sum('connections').alias('connections'))
        return connections

def main():
    pass

if __name__ == '__main__':
    main()