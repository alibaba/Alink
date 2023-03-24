# 分组EM (GroupEmBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.GroupEmBatchOp

Python 类名：GroupEmBatchOp


## 功能介绍

分组EM聚类算法。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| groupCols | 分组列名，多列 | 分组列名，多列，必选 | String[] | ✓ |  |  |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK" | "EUCLIDEAN" |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  |  | 1.0E-4 |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  |  | 10 |

