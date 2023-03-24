# Dbscan训练 (Dbscan)
Java 类名：com.alibaba.alink.pipeline.clustering.Dbscan

Python 类名：Dbscan


## 功能介绍

[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。
<div align=center><img src="https://img.alicdn.com/tfs/TB19W7uaW61gK0jSZFlXXXDKFXa-700-431.png" height="50%" width="50%"></div>

本算法为DBSCAN对应的训练组件，输入为训练数据，输出有两个：（1）每个数据点的ID，节点类型以及聚类中心，（2）可以用来预测新数据的DBSCAN模型。

### 距离度量方式
| 参数名称 | 参数描述 | 说明 |
| --- | --- | --- |
| EUCLIDEAN | <img src="https://img.alicdn.com/imgextra/i3/O1CN01fpjOXj1rCYlGWvP7w_!!6000000005595-2-tps-1624-290.png"> | 欧式距离 |
| COSINE | <img src="https://img.alicdn.com/imgextra/i2/O1CN01w80YFW1tl1nZ5qEBN_!!6000000005941-2-tps-1676-330.png"> | 夹角余弦距离 |
| CITYBLOCK | <img src="https://img.alicdn.com/imgextra/i4/O1CN01aTf1Zt1aVw2vjuYw1_!!6000000003336-2-tps-1666-280.png"> | 街区距离 |

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| epsilon | 邻域距离阈值 | 邻域距离阈值 | Double | ✓ |  |  |
| idCol | ID列名 | ID列对应的列名 | String | ✓ |  |  |
| minPoints | 邻域中样本个数的阈值 | 邻域中样本个数的阈值 | Integer | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK" | "EUCLIDEAN" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

