from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('bfs_degree_of_seperation')
sc = SparkContext(conf = conf)

class RDD:
    def __init__(self):
        pass

    def convert_to_bfs(self):
        pass
