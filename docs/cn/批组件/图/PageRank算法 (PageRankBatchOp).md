# PageRank算法 (PageRankBatchOp)
Java 类名：com.alibaba.alink.operator.batch.graph.PageRankBatchOp

Python 类名：PageRankBatchOp


## 功能介绍

计算 Page Rank

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| edgeSourceCol | 边表中起点所在列 | 边表中起点所在列 | String | ✓ |  |  |
| edgeTargetCol | 边表中终点所在列 | 边表中终点所在列 | String | ✓ |  |  |
| dampingFactor | 阻尼系数 | 阻尼系数 | Double |  |  | 0.85 |
| edgeWeightCol | 边权重列 | 表示边权重的列 | String |  |  | null |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  |  | 1.0E-4 |
| maxIter | 最大迭代次数 | 最大迭代次数 | Integer |  | x >= 1 | 50 |

