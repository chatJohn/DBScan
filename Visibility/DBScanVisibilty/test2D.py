import os
import string

import matplotlib.pyplot as plt

from DBScanVisibilty.utils.isFloat import is_float


class ResultMap:
    def __init__(self, dir_path: string, option: int):
        self.dir_path = dir_path
        # 假设聚类结果存储在一个名为"labels"的列表中
        self.labels = []
        # 假设数据点的坐标存储在一个名为"points"的列表中
        self.points = []
        self.labels_with_points = []
        self.clusters = {}
        self.colors = ['y', 'r', 'g', 'b', 'c', 'm', 'k']  # 指定不同类别的颜色
        self.option = option
    def readFiles(self):
        files = os.listdir(self.dir_path)
        for file in files:
            file_path = os.path.join(self.dir_path, file)
            if os.path.isfile(file_path) and file_path.endswith(".txt"):
                with open(file_path, "r") as openFile:
                    line = openFile.readline()
                    while line:
                        if self.option == 0: # 处理聚类的2D结果
                            ls = line.split(", ")
                            label = -1
                            point = ()
                            if ls[1].isnumeric():
                                label = int(ls[1])
                            ls[0] = ls[0].lstrip("[").rstrip("]").split(",")
                            if is_float(ls[0][0]) and is_float(ls[0][1]):
                                point = (float(ls[0][0]), float(ls[0][1]))
                            if label != -1 and point != ():
                                if label not in self.clusters:
                                    self.clusters[label] = []
                                self.clusters[label].append(point)
                                if self.labels.count(label) == 0:
                                    self.labels.append(label)
                                self.points.append(point)
                            line = openFile.readline()
                        else: # 处理原始数据
                            ls = line.split(",")
                            label = -1
                            point = ()
                            if ls[0].isnumeric():
                                label = int(ls[0])
                            if is_float(ls[2]) and is_float(ls[3]):
                                point = (float(ls[2]), float(ls[3]))
                            if label != -1 and point != ():
                                if label not in self.clusters:
                                    self.clusters[label] = []
                                self.clusters[label].append(point)
                                if self.labels.count(label) == 0:
                                    self.labels.append(label)
                                self.points.append(point)
                            line = openFile.readline()
    def draw_pic(self):
        for label, cluster_points in self.clusters.items():
            x = [point[0] for point in cluster_points]
            y = [point[1] for point in cluster_points]
            plt.scatter(x, y, s=10, c=self.colors[(label % 7)], label=f'Cluster {label}', marker='*')
        plt.xlabel('X')
        plt.ylabel('Y')
        plt.title('Clustering Result')
        plt.legend()
        plt.show()

if __name__ == "__main__":
    folder_path = "F:\\PyCharm Community Edition 2022.3.2\\MachineLearing\\resData"
    data_path = "F:\\PyCharm Community Edition 2022.3.2\\MachineLearing\\Data"
    res1 = ResultMap(folder_path, 0)
    res = ResultMap(data_path, 1)
    res1.readFiles()
    res1.draw_pic()