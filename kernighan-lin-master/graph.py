class Graph:
    nodes = {}
    matrix = []

    def __init__(self, graph_dict):
        temp_dict = {}
        for i in range(len(graph_dict['nodes'])):
            self.nodes[i] = graph_dict['nodes'][i]
            temp_dict[graph_dict['nodes'][i]] = i

        self.matrix = [[0 for x in graph_dict['nodes']] for y in graph_dict['nodes']]
        for i in graph_dict['edges']:
            self.matrix[temp_dict.get(i[0])][temp_dict.get(i[1])] = i[2]
            self.matrix[temp_dict.get(i[1])][temp_dict.get(i[0])] = i[2]

    def getSize(self):
        return len(self.matrix)

    def getWeight(self, node1, node2):
        return self.matrix[node1][node2]

    def getNodeLabel(self, node):
        return self.nodes.get(node)
