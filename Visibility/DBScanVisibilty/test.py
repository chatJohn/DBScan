import os
import matplotlib.pyplot as plt

class DBScanRectangle:
    def __init__(self, leftDownX, leftDownY, rightUpX, rightUpY):
        self.leftDownX = leftDownX
        self.leftDownY = leftDownY
        self.rightUpX = rightUpX
        self.rightUpY = rightUpY

def visualize(rectangles,clusters):
    plt.figure()
    
    # 分区结果
    colors = ['red', 'blue', 'green']
    for rect, color in zip(rectangles, colors):
        x = [rect.leftDownX, rect.rightUpX, rect.rightUpX, rect.leftDownX, rect.leftDownX]
        y = [rect.leftDownY, rect.leftDownY, rect.rightUpY, rect.rightUpY, rect.leftDownY]
        plt.scatter(x, y, c=color)
        plt.plot([x[0], x[1], x[2], x[3], x[0]], [y[0], y[1], y[2], y[3], y[0]], c=color, linestyle='-')  
    
    # 聚类结果
    colors = ['y', 'r', 'g', 'b', 'c', 'm', 'k',"orange","lightcoral","fuchsia","slategray"]  # 指定不同类别的颜色
    for label, cluster_points in clusters.items():
        x = [point[0] for point in cluster_points]
        y = [point[1] for point in cluster_points]
        plt.scatter(x, y, s=10, c=colors[label], label=f'Cluster {label}', marker='*')

    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('Partitons and Clusters')
    # plt.legend()
    plt.show()

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
    
# 假设聚类结果存储在一个名为"labels"的列表中
labels = []
# 假设数据点的坐标存储在一个名为"points"的列表中
points = []
labels_with_points = []
clusters = {}

file_path = "D:\\START\\distribute-ST-cluster\\code\\DBScan-VeG\\Visibility\\resData\\10_15fil.txt"
with open(file_path, "r") as openFile:
    line = openFile.readline()
    while line:
        ls = line.split(", ")
        label = -1
        point = ()
        if ls[1].isnumeric():
            label = int(ls[1])
        ls[0] = ls[0].lstrip("[").rstrip("]").split(",")
        if is_float(ls[0][0]) and is_float(ls[0][1]):
            point = (float(ls[0][0]), float(ls[0][1]))
        if label != -1 and point != ():
            if label not in clusters:
                clusters[label] = []
            clusters[label].append(point)
            labels_with_points.append((label, point))
            if labels.count(label) == 0:
                labels.append(label)
            points.append(point)
        line = openFile.readline()

rect1 = DBScanRectangle(116.5, 30.7, 116.7, 40.3)
rect2 = DBScanRectangle(111.7, 39.9, 116.5, 40.3)
rect3 = DBScanRectangle(111.7, 30.7, 116.4, 39.8)
rectangles = [rect1, rect2, rect3]

visualize(rectangles,clusters)
