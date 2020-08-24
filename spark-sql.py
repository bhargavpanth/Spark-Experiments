from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('test_spark_sql').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile('./datasets/fakefriends.csv')
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

def get_people_of_different_ages():
    pre_teens = spark.sql('SELECT * FROM people WHERE age >= 10 AND age < 13')
    teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')
    young_adults = spark.sql('SELECT * FROM people WHERE age >= 19 AND age <= 23')
    adults = spark.sql('SELECT * FROM people WHERE age > 24')
    return [pre_teens, teenagers, young_adults, adults]

schemaPeople.groupBy('age').count().orderBy('age').show()

spark.stop()
