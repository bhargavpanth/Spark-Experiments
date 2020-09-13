from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('bfs_degree_of_seperation')
sc = SparkContext(conf = conf)

hit_counter = sc.accumulator(0)

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

    def convert_to_bfs(self):
        return self.convert_to_bfs()

def bfs_map(node):
    target_id = 14
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]
    results = []
    if (color == 'GRAY'):
        for connection in connections:
            new_ID = connection
            new_distance = distance + 1
            new_color = 'GRAY'
            if (target_id == connection):
                hit_counter.add(1)

            entry = (new_ID, ([], new_distance, new_color))
            results.append(entry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (character_id, (connections, distance, color)) )
    return results

def main():
    iteration_rdd = RDD().convert_to_bfs()
    for iteration in range(0, 10):
        print('Running BFS iteration# ' + str(iteration+1))
