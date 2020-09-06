from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class Graph:
    def __init__(self):
        self.spark = SparkSession.builder.appName('degree_of_seperation').getOrCreate()
        self.schema = StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True)])

    def read(self):
        pass

def main():
    pass

if __name__ == '__main__':
    main()