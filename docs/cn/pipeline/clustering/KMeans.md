## 功能介绍
KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 50。 | Integer |  | 50 |
| initMode | 中心点初始化方法 | 初始化中心点的方法，支持"K_MEANS_PARALLEL"和"RANDOM" | String |  | "RANDOM" |
| initSteps | k-means++初始化迭代步数 | k-means初始化中心点时迭代的步数 | Integer |  | 2 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  | 2 |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  | 1.0E-4 |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | 0 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



## 脚本示例
#### 脚本代码
```
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "0.1,0.1,0.1"],
    [2, "0.2,0.2,0.2"],
    [3, "9 9 9"],
    [4, "9.1 9.1 9.1"],
    [5, "9.2 9.2 9.2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
kmeans = KMeans().setVectorCol("vec").setK(2).setPredictionCol("pred")
kmeans.fit(inOp).transform(inOp).collectToDataframe()
```

#### 脚本运行结果
##### 预测结果
```
rowID   id          vec  pred
0   0        0 0 0     1
1   1  0.1,0.1,0.1     1
2   2  0.2,0.2,0.2     1
3   3        9 9 9     0
4   4  9.1 9.1 9.1     0
5   5  9.2 9.2 9.2     0
```
