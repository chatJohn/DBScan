import numpy as np
class Graph:
    nodes = set()
    matrix = np.zeros((200, 200))
    weightsum = 0

    def __init__(self, path):
        with open(path) as text:
            for line in text:
                vertices = line.strip().split()
                v_i = int(vertices[0])
                v_j = int(vertices[1])
                w = float(vertices[2])
                self.nodes.add(v_i)
                self.nodes.add(v_j)
                self.matrix[v_i][v_j] = w
                self.matrix[v_j][v_i] = w
                self.weightsum += 2*w

    def getSize(self):
        return len(self.nodes)

    def getWeight(self, node1, node2):
        return self.matrix[node1][node2]