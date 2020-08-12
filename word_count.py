from pyspark import SparkConf, SparkContext
import re

class WordCount:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
        self.sc = SparkContext(conf = self.conf)
        self.file = './datasets/modern_prometheus.txt'

    def load(self):
        return self.sc.textFile(self.file)

    def regularize_text(self, text):
        return re.compile(r'\W+', re.UNICODE).split(text.lower())

    def read_book(self):
        lines = self.load()
        # self.regularize_text
        words = lines.flatMap(lambda x: x.split())
        return words.countByValue()

    def plot(self):
        # map over the RDD
        print(self.read_book())

def main():
    WordCount().plot()

if __name__ == '__main__':
    main()