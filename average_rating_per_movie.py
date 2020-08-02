from pyspark import SparkConf, SparkContext

class AverageMovieRating:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
        self.sc = SparkContext(conf = self.conf)
        self.file = './ml-100k/u.data'

    def load(self):
        return self.sc.textFile(self.file)

    def read(self):
        lines = self.load()
        ratings = lines.mapValues(lambda x: x.split()[2], x.split()[1])
        return ratings.countByValue()

