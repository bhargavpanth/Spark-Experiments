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


# class Session:
#     def __init__(self):
#         self.spark = SparkSession.builder.appName('test_spark_sql').getOrCreate()
#         self.file = './datasets/fakefriends.csv'

#     def mapper(self, data):
#         fields = data.split(',')
#         return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

#     def load(self):
#         return self.spark.sparkContext.textFile(self.file)

#     def read(self):
#         lines = self.load()
#         people = lines.map(self.mapper)
#         self.schema = self.spark.createDataFrame(people).cache()
#         self.schema.createOrReplaceTempView('people_schema')

#     def get_people_of_different_ages(self):
#         self.read()
#         pre_teens = self.spark.sql('SELECT * FROM people WHERE age >= 10 AND age < 13')
#         teenagers = self.spark.sql('SELECT * FROM people WHERE age >= 13 AND age <= 19')
#         young_adults = self.spark.sql('SELECT * FROM people WHERE age >= 19 AND age <= 23')
#         adults = self.spark.sql('SELECT * FROM people WHERE age > 24')
#         return [pre_teens, teenagers, young_adults, adults]

#     def show_all(self):
#         return self.get_people_of_different_ages()

#     def group_by_age(self):
#         return self.schema.groupBy('age').count().orderBy('age').show()

# def main():
#     [pre_teens, teenagers, young_adults, adults] = Session().get_people_of_different_ages()
#     for teens in teenagers:
#         print(teens)

# if __name__ == '__main__':
#     main()

