from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class MinTemp:
    def __init__(self):
        self.spark = SparkSession.builder.appName('min_temperature').getOrCreate()
        self.file = './dataset/1800.csv'

    def schema(self):
        return StructType([\
            StructField('stationID', StringType(), True), \
            StructField('date', IntegerType(), True), \
            StructField('measure_type', StringType(), True), \
            StructField('temperature', FloatType(), True) \
        ])

    def read(self):
        schema = self.schema()
        return self.spark.read.schema(schema).csv(self.file)

    def min_temp(self):
        df = self.read()
        minTemps = df.filter(df.measure_type == 'TMIN')
        stationTemps = minTemps.select('stationID', 'temperature')
        minTempsByStation = stationTemps.groupBy('stationID').min('temperature')
        minTempsByStation.show()
        # converting temperature
        minTempsByStationF = minTempsByStation.withColumn('temperature', func.round(func.col('min(temperature)') * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                        .select('stationID', 'temperature').sort('temperature')
        results = minTempsByStationF.collect()
        for result in results:
            print(result[0] + '\t{:.2f}F'.format(result[1]))
        self.spark.stop()

def main():
    MinTemp().min_temp()

if __name__ == '__main__':
    main()

