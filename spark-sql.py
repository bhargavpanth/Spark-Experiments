from pyspark.sql import SparkSession
from pyspark.sql import Row

class Session:
    def __init__(self):
        self.spark = SparkSession.builder.appName('test_spark_sql').getOrCreate()
        self.file = './datasets/fakefriends.csv'

    def mapper(self, data):
        fields = data.split(',')
        return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

    def load(self):
        return self.spark.sparkContext.textFile(self.file)

    def read(self):
        lines = self.load()
        people = lines.map(self.mapper)
        schema = self.spark.createDataFrame(people).cache()
        schema.createOrReplaceTempView('people_schema')


# spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# def mapper(line):
#     fields = line.split(',')
#     return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
#                age=int(fields[2]), numFriends=int(fields[3]))

# lines = spark.sparkContext.textFile("fakefriends.csv")
# people = lines.map(mapper)

# # Infer the schema, and register the DataFrame as a table.
# schemaPeople = spark.createDataFrame(people).cache()
# schemaPeople.createOrReplaceTempView("people")

# # SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# # The results of SQL queries are RDDs and support all the normal RDD operations.
# for teen in teenagers.collect():
#   print(teen)

# # We can also use functions instead of SQL queries:
# schemaPeople.groupBy("age").count().orderBy("age").show()

# spark.stop()


