## 功能介绍
二分k均值算法是k-means聚类算法的一个变体，主要是为了改进k-means算法随机选择初始质心的随机性造成聚类结果不确定性的问题.

## 参数说明
<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| minDivisibleClusterSize | 最小可分裂的聚类大小 | 最小可分裂的聚类大小 | Integer |  | 1 |
| k | 聚类中心点数目 | 聚类中心点数目 | Integer |  | 4 |
| distanceType | 距离度量方式 | 聚类使用的距离类型，支持EUCLIDEAN（欧式距离）和 COSINE（余弦距离） | String |  | "EUCLIDEAN" |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 10。 | Integer |  | 10 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码
```python
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
kmeans = BisectingKMeans().setVectorCol("vec").setK(2).setPredictionCol("pred")
kmeans.fit(inOp).transform(inOp).collectToDataframe()
```

#### 脚本运行结果
##### 预测结果
```
rowId	id	vec	pred
0	0	0 0 0	0
1	1	0.1,0.1,0.1	0
2	2	0.2,0.2,0.2	0
3	3	9 9 9	1
4	4	9.1 9.1 9.1	1
5	5	9.2 9.2 9.2	1
```