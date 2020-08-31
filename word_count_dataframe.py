from pyspark.sql import SparkSession
from pyspark.sql import functions as func

class WordCount:
    def __init__(self):
        self.spark = SparkSession.builder.appName('WordCount').getOrCreate()
        self.inputDF = self.spark.read.text('./datasets/modern_prometheus.txt')

    def normalize(self):
        words = self.inputDF.select(func.explode(func.split(self.inputDF.value, "\\W+")).alias("word"))
        words.filter(words.word != '')
        return words.select(func.lower(words.word).alias('word'))

    def sorted_word_count(self):
        lower_case_words = self.normalize()
        wordCounts = lower_case_words.groupBy('word').count()
        # Sort by counts
        wordCountsSorted = wordCounts.sort('count')
        wordCountsSorted.show(wordCountsSorted.count())

def main():
    WordCount().sorted_word_count()

if __name__ == '__main__':
    main()
