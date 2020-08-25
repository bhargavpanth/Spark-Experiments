from pyspark.sql import SparkSession
from pyspark.sql import Row

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode('utf-8')), age=int(fields[2]), numFriends=int(fields[3]))

# class Session:
#     def __init__(self):
#         self.spark = SparkSession.builder.appName('test_spark_sql').getOrCreate()
#         self.file = './datasets/fakefriends.csv'

#     def load(self):
#         return self.spark.sparkContext.textFile(self.file)
        
#     def get_people_of_different_ages(self):
#         pre_teens = self.spark.sql('SELECT * FROM people WHERE age >= 10 AND age < 13')
#         teenagers = self.spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')
#         young_adults = self.spark.sql('SELECT * FROM people WHERE age >= 19 AND age <= 23')
#         adults = self.spark.sql('SELECT * FROM people WHERE age > 24')
#         return [pre_teens, teenagers, young_adults, adults]

spark = SparkSession.builder.appName('test_sparkql').getOrCreate()

lines = spark.sparkContext.textFile('./datasets/fakefriends.csv')
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy('age').count().orderBy('age').show()

spark.stop()