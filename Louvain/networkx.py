import networkx as nx
import matplotlib.pyplot as plt

# 读取文本文件，创建图
def get_graph(file_path):
    G = nx.Graph()
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split()
            source = parts[0]
            target = parts[1]
            weight = float(parts[2])  # 假设权重是浮点数
            G.add_edge(source, target, weight=weight)
    return G

# 绘制图
def draw_graph(G):
    pos = nx.spring_layout(G)  # 选择布局算法
    nx.draw(G, pos, with_labels=True, node_color='skyblue', node_size=50, edge_color='k', linewidths=1, font_size=5)
    labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=labels,font_size=5)
    plt.show()

# 主程序
if __name__ == "__main__":
    file_path = 'edges_all_delete.txt'
    graph = get_graph(file_path)
    draw_graph(graph)
