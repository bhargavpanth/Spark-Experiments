from pyspark import SparkConf, SparkContext
import collections

class WordCount:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
        self.sc = SparkContext(conf = self.conf)
        self.file = './datasets/modern_prometheus.txt'

    def load(self):
        return self.sc.textFile(self.file)

    def read_book(self):
        lines = self.load()
        words = lines.flatMap(lambda x: x.split())
        return words.countByValue()

    def plot(self):
        # map over the RDD
        print(self.read_book())

def main():
    WordCount().plot()

if __name__ == '__main__':
    main()