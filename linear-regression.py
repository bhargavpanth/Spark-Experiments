from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('linear_regression').getOrCreate()
lines = spark.sparkContext.textFile('./datasets/regression.txt')
data = lines.map(lambda x: x.split(',')).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))


def main():
    pass

if __name__ == '__main__':
    main()