from pyspark import SparkConf, SparkContext
import collections

class Filter:
    def __init__(self):
        self.conf = SparkConf().setMaster('local').setAppName('ratings_histogram')
        self.sc = SparkContext(conf = self.conf)
        self.file = './ml-100k/u.data'

    