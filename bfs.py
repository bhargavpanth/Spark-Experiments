from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('bfs_degree_of_seperation')
sc = SparkContext(conf = conf)
