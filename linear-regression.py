from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName('linear_regression').getOrCreate()
lines = spark.sparkContext.textFile('./datasets/regression.txt')
data = lines.map(lambda x: x.split(',')).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))


def main():
    columns = ['label', 'features']
    df = data.toDF(columns)

    train_test = df.randomSplit([0.5, 0.5])
    training_df = train_test[0]
    testing_df = train_test[1]
    linear_regression = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = linear_regression.fit(training_df)
    fullPredictions = model.transform(testing_df).cache()

    predictions = fullPredictions.select('prediction').rdd.map(lambda x: x[0])
    labels = fullPredictions.select('label').rdd.map(lambda x: x[0])

    predictionAndLabel = predictions.zip(labels).collect()
    for prediction in predictionAndLabel:
      print(prediction)
    spark.stop()

if __name__ == '__main__':
    main()
