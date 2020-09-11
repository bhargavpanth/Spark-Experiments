from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class Graph:
    def __init__(self):
        self.spark = SparkSession.builder.appName('degree_of_seperation').getOrCreate()
        self.schema = StructType([StructField('id', IntegerType(), True), StructField('name', StringType(), True)])

    def read(self):
        schema = self.schema
        names = self.spark.read.schema(schema).option('sep', ' ').csv('./datasets/names.txt')
        lines = self.spark.read.text('./datasets/graph.txt')
        return [names, lines]

    def connections(self):
        [names, lines] = self.read()
        connections = lines.withColumn('id', func.split(func.col('value'), ' ')[0]) \
            .withColumn('connections', func.size(func.split(func.col('value'), ' ')) - 1) \
            .groupBy('id').agg(func.sum('connections').alias('connections'))
        return connections

    def most_popular(self):
        connections = self.connections()
        [names, lines] = self.read()
        most_popular = connections.sort(func.col('connections').desc()).first()
        most_popular_name = names.filter(func.col('id') == most_popular[0]).select('name').first()
        print(most_popular_name[0] + ' is the most popular superhero with ' + str(most_popular[1]) + ' co-appearances.')

    def most_obscure(self):
        connections = self.connections()
        [names, lines] = self.read()
        min_connection_count = connections.agg(func.min('connections')).first()[0]
        min_connections = connections.filter(func.col('connections') == min_connection_count)
        min_connections_with_names = min_connections.join(names, 'id')
        print('The following characters have only ' + str(min_connection_count) + ' connection(s):')
        min_connections_with_names.select('name').show()

def main():
    Graph().most_popular()
    '''
    CAPTAIN AMERICA is the most popular superhero with 1937 co-appearances.
    '''
    Graph().most_obscure()
    '''
    
    '''

if __name__ == '__main__':
    main()