from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('bfs_degree_of_seperation')
sc = SparkContext(conf = conf)

class RDD:
    def __init__(self):
        self.text_file = sc.textFile('./dataset/graph.txt')
        self.bfs = self.text_file.map(self.__convert_to_bfs)

    def __convert_to_bfs(self, line):
        start_id = 5306
        fields = line.split()
        person_id = int(fields[0])
        connections = []
        for connection in fields[1:]:
            connections.append(int(connection))
            
        colour = 'WHITE'
        distance = 9999
        
        if (person_id == start_id):
            colour = 'GRAY'
            distance = 0
            
        return (person_id, (connections, distance, colour))
