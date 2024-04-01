import json
import sys

from graph import Graph


def createGraph():
    with open('graph.json', 'r') as f:
        graph_dic = json.load(f)

    return Graph(graph_dic)


def sumWeights(graph, internalSet, node):
    weights = 0
    for i in internalSet:
        weights += graph.getWeight(node, i)
    return weights


def reduction(graph, internal, external, node):
    return sumWeights(graph, external, node) - sumWeights(graph, internal, node)


def computeD(graph, A, B):
    D = {}
    for i in A:
        D[i] = reduction(graph, A, B, i)
    for i in B:
        D[i] = reduction(graph, B, A, i)
    return D


def maxSwitchCostNodes(graph, A, B, D):
    maxCost = -sys.maxsize - 1
    a = None
    b = None
    for i in A:
        for j in B:
            cost = D[i] + D[j] - 2 * graph.getWeight(i, j)
            if cost > maxCost:
                maxCost = cost
                a = i
                b = j

    return a, b, maxCost


def updateD(graph, A, B, D, a, b):
    for i in A:
        D[i] = D[i] + graph.getWeight(i, a) - graph.getWeight(i, b)
    for i in B:
        D[i] = D[i] + graph.getWeight(i, b) - graph.getWeight(i, a)
    return D


def getMaxCostAndIndex(costs):
    maxCost = -sys.maxsize - 1
    index = 0
    sum = 0

    for i in costs:
        sum += i
        if sum > maxCost:
            maxCost = sum
            index = costs.index(i)

    return maxCost, index


def switch(graph, A, B):
    D = computeD(graph, A, B)
    costs = []
    X = []
    Y = []

    for i in range(int(graph.getSize() / 2)):
        print("A",A,"B",B)
        x, y, cost = maxSwitchCostNodes(graph, A, B, D)
        print("x",x,"y",y)
        A.remove(x)
        B.remove(y)

        costs.append(cost)
        X.append(x)
        Y.append(y)
        print("X",X,"Y",Y)
        D = updateD(graph, A, B, D, x, y)
    print("done---")
    maxCost, k = getMaxCostAndIndex(costs)

    if maxCost > 0:
        A = Y[:k + 1] + X[k + 1:]
        B = X[:k + 1] + Y[k + 1:]
        return A, B, False
    else:
        A = [i for i in X]
        B = [i for i in Y]
        return A, B, True


def k_lin():
    graph = createGraph()
    A = [i for i in range(int(graph.getSize() / 2))]
    B = [i for i in range(int(graph.getSize() / 2), graph.getSize())]
    done = False
    print("A",A,"B",B)
    while not done:
        A, B, done = switch(graph, A, B)

    print("Partition A: ")
    for i in A:
        print(graph.getNodeLabel(i))
    print("\nPartition B: ")
    for i in B:
        print(graph.getNodeLabel(i))


def main():
    k_lin()


if __name__ == '__main__':
    main()
