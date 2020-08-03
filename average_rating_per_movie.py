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
        total_rating_by_movie = lines.mapValues(lambda x: x.split()[2], x.split()[1]).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        return total_rating_by_movie.mapValues(lambda x: x[0]/x[1])

    def plot(self):
        average_rating_per_movie = self.read()
        results = average_rating_per_movie.collect()
        for result in results:
            print(result)




