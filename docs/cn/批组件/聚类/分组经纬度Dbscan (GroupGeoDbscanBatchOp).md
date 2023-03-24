# 分组经纬度Dbscan (GroupGeoDbscanBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.GroupGeoDbscanBatchOp

Python 类名：GroupGeoDbscanBatchOp


## 功能介绍
[DBSCAN](https://en.wikipedia.org/wiki/DBSCAN)，Density-Based Spatial Clustering of Applications with Noise，是一个比较有代表性的基于密度的聚类算法。与划分和层次聚类方法不同，它将簇定义为密度相连的点的最大集合，能够把具有足够高密度的区域划分为簇，并可在噪声的空间数据库中发现任意形状的聚类。

按经纬度分组DBSCAN算法根据用户指定的"分组列"将输入数据分为多个组，然后在每个组内部按经纬度进行DBSCAN聚类算法。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| epsilon | 邻域距离阈值 | 邻域距离阈值 | Double | ✓ |  |  |
| groupCols | 分组列名，多列 | 分组列名，多列，必选 | String[] | ✓ |  |  |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| latitudeCol | 经度列名 | 经度列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| longitudeCol | 纬度列名 | 纬度列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| minPoints | 邻域中样本个数的阈值 | 邻域中样本个数的阈值 | Integer | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| groupMaxSamples | 每个分组的最大样本数 | 每个分组的最大样本数 | Integer |  |  | 2147483647 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| skip | 每个分组超过最大样本数时，是否跳过 | 每个分组超过最大样本数时，是否跳过 | Boolean |  |  | false |

