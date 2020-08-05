from pyspark import SparkConf, SparkContext
import collections

class Filter:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
        self.sc = SparkContext(conf = self.conf)
        self.file = './ml-100k/u.data'

    def load(self):
        return self.sc.textFile(self.file)

    def read_ratings_greater_than(self, rating):
        lines = self.load()
        ratings = lines.filter(lambda x: x.split()[2] >= int(rating))
        return ratings.countByValue()

    def plot(self):
        result = self.read_ratings_greater_than(3)
        sortedResults = collections.OrderedDict(sorted(result.items()))
        for key, value in sortedResults.items():
            print('%s %i' % (key, value))

def main():
    Filter().plot()

if __name__ == '__main__':
    main()
