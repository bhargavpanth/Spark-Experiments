from pyspark import SparkConf, SparkContext
import collections

class WordCount:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
        self.sc = SparkContext(conf = self.conf)
        self.file = './datasets/modern_prometheus.txt'