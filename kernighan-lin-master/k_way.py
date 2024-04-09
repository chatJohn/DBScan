import sys

from k_way_graph import Graph

def load_graph(path):
    return Graph(path)

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
    # print("------------")
    for i in A:
        for j in B:
            cost = D[i] + D[j] - 2 * graph.getWeight(i, j)
            if cost > maxCost:
                maxCost = cost
                a = i
                b = j
            # print("cost",cost,"a",a,"b",b)

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


def switch(graph, A, B,k):
    D = computeD(graph, A, B)
    costs = []
    X = []
    Y = []

    for i in range(int(graph.getSize() / k)+1):
        # print("A",A,"B",B)
        x, y, cost = maxSwitchCostNodes(graph, A, B, D)
        if x is not None and y is not None:
            # print("x",x,"y",y)
            A.remove(x)
            B.remove(y)
            costs.append(cost)
            X.append(x)
            Y.append(y)
            # print("X",X,"Y",Y)
            D = updateD(graph, A, B, D, x, y)
        elif len(B)>0:
            # print("x",x,"y",y)
            Y.append(B[0])
            B.remove(B[0])
            # print("X",X,"Y",Y)
        elif len(A)>0:
            # print("x",x,"y",y)
            X.append(A[0])
            A.remove(A[0])
            # print("X",X,"Y",Y)
    # print("done---")
    maxCost, index = getMaxCostAndIndex(costs)

    if maxCost > 0:
        A = Y[:index + 1] + X[index + 1:]
        B = X[:index + 1] + Y[index + 1:]
        return A, B, False
    else:
        A = [i for i in X]
        B = [i for i in Y]
        return A, B, True

def k_lin(k,path):
    graph = load_graph(path)

    partitions = {i: [] for i in range(k)}
    partition_index = 0
    # print(graph.getSize())
    # print(graph.nodes)
    for i in range(1,graph.getSize()+1):
        partitions[partition_index % k].append(i)
        partition_index = partition_index + 1

    print("\nBefore KL")
    sum_all = 0
    for i in range(k):
        print("\nPartition", i, ":")
        sum = 0
        for node in partitions[i]:
            print(node,end=' ')
            sum += sumWeights(graph, partitions[i], node)
        print("internal weight",sum)
        sum_all += sum
    print("external weight",graph.weightsum-sum_all)

    for i in partitions:
        for j in partitions:
            if(i<j):
                done = False
                while not done:
                    partitions[i],partitions[j],done = switch(graph,partitions[i],partitions[j],k)

    print("After KL")
    sum_all = 0
    for i in range(k):
        print("\nPartition", i, ":")
        sum = 0
        for node in partitions[i]:
            print(node,end=' ')
            sum += sumWeights(graph, partitions[i], node)
        print("internal weight",sum)
        sum_all += sum
    print("external weight",graph.weightsum-sum_all)

def main():
    k = 12  # 设定分区数
    path= "edges.txt"
    k_lin(k,path)

if __name__ == '__main__':
    main()
