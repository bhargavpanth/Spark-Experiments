from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('linear_regression').getOrCreate()

def main():
    pass

if __name__ == '__main__':
    main()