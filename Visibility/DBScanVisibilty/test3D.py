import os
import string
from datetime import datetime

from DBScanVisibilty.utils.isFloat import is_float
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

class ResultMap3D:
    def __init__(self, dir_path: string, option: int):
        # 标准时间
        self.referTime = datetime(2008, 2, 2, 18, 44, 58)
        self.time_format = '%Y-%m-%d %H:%M:%S'
        self.dir_path = dir_path
        self.option = option
        # 将聚类结果储存在一个label的列表之中
        self.labels = []
        # 将数据点储存在一个points的列表之中
        self.points = []
        # 将数据点的时间储存在一个time_span之中
        self.time_span = []
        self.clusters = {}  # {label: (time, [x, y])}
        self.colors = ['y', 'r', 'g', 'b', 'c', 'm', 'k']  # 指定不同类别的颜色
    def readFiles(self):
        files = os.listdir(self.dir_path)
        for file in files:
            file_path = os.path.join(self.dir_path, file)
            if os.path.isfile(file_path) and file_path.endswith(".txt"):
                with open(file_path, "r") as openFile:
                    line = openFile.readline()
                    while line:
                        if self.option == 0: # 原始数据可视化
                            ls = line.split(",")
                            label = -1
                            point = ()
                            tp = ls[1]
                            t = datetime.strptime(tp, self.time_format)
                            diff_second = (t - self.referTime).days * 86400 + (t - self.referTime).seconds
                            if self.time_span.count(t) == 0:
                                self.time_span.append(t)
                            print(t)
                            if ls[0].isnumeric():
                                label = int(ls[0])
                            if is_float(ls[2]) and is_float(ls[3]):
                                point = (float(ls[2]), float(ls[3]))
                            if label != -1 and point != ():
                                if label not in self.clusters:
                                    self.clusters[label] = []
                                self.clusters[label].append((diff_second, point))
                                if self.labels.count(label) == 0:
                                    self.labels.append(label)
                                self.points.append(point)
                            line = openFile.readline()
                        elif self.option == 1: # 聚类结果3D可视化
                            ls = line.split(", ")
                            label = -1
                            point = ()
                            if ls[1].isnumeric():
                                label = int(ls[1])
                            ls[0] = ls[0].lstrip("[").rstrip("]").split(",")
                            diff_second = float(ls[0][2])
                            if is_float(ls[0][0]) and is_float(ls[0][1]):
                                point = (float(ls[0][0]), float(ls[0][1]))
                            if label != -1 and point != ():
                                if label not in self.clusters:
                                    self.clusters[label] = []
                                self.clusters[label].append((diff_second, point))
                                if self.labels.count(label) == 0:
                                    self.labels.append(label)
                                self.points.append(point)
                            line = openFile.readline()
    def draw_3D(self):
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        for label, time_point in self.clusters.items():
            t = [tp[0] for tp in time_point]
            x = [tp[1][0] for tp in time_point]
            y = [tp[1][1] for tp in time_point]
        ax.scatter(x, y, t, c=self.colors[(label % 7)], marker='o')
        # 设置坐标轴标签
        ax.set_xlabel('point_X')
        ax.set_ylabel('point_Y')
        ax.set_zlabel('time')
        plt.show()
if __name__ == "__main__":
    folder_path = "F:\\PyCharm Community Edition 2022.3.2\\MachineLearing\\resData"
    data_path = "F:\\PyCharm Community Edition 2022.3.2\\MachineLearing\\data"
    res = ResultMap3D(folder_path, 1)
    res.readFiles()
    res.draw_3D()