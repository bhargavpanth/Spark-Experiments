from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('spark-sql-header').getOrCreate()

people = spark.read.option('header', 'true').option('inferSchema', 'true').csv('./datasets/fakefriends-header.csv')

# printing schema
people.printSchema()
# property show
people.select('name').show()
#
people.filter(people.age < 21).show()

spark.stop()

