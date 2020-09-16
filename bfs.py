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

def bfs_reduce(node_one, node_two):
    edges1 = node_one[0]
    edges2 = node_two[0]
    distance_one = node_one[1]
    distance_two = node_two[1]
    colour_one = node_one[2]
    colour_two = node_two[2]

    distance = 9999
    color = colour_one
    edges = []

    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Minimum distance
    if (distance_one < distance):
        distance = distance_one

    if (distance_two < distance):
        distance = distance_two

    if (colour_one == 'WHITE' and (colour_two == 'GRAY' or colour_two == 'BLACK')):
        color = colour_two

    if (colour_one == 'GRAY' and colour_two == 'BLACK'):
        color = colour_two

    if (colour_two == 'WHITE' and (colour_one == 'GRAY' or colour_one == 'BLACK')):
        color = colour_one

    if (colour_two == 'GRAY' and colour_one == 'BLACK'):
        color = colour_one

    return (edges, distance, color)

def main():
    iteration_rdd = RDD().convert_to_bfs()
    for iteration in range(0, 10):
        print('Running BFS iteration# ' + str(iteration+1))
        mapped = iteration_rdd.flatMap(bfs_map)
        print('Processing ' + str(mapped.count()) + ' values')
        if (hit_counter.value > 0):
            print('Hit from ' + str(hit_counter.value) + 'different direction(s).')
            break
        # Reducer combines data for each character ID, preserving the darkest
        # color and shortest path.
        iteration_rdd = mapped.reduceByKey(bfs_reduce)

if __name__ == '__main__':
    main()