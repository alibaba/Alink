## 功能介绍
KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| distanceType | 距离度量方式 | 聚类使用的距离类型，支持EUCLIDEAN（欧式距离）和 COSINE（余弦距离） | String |  | "EUCLIDEAN" |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 20。 | Integer |  | 20 |
| initMode | 中心点初始化方法 | 初始化中心点的方法，支持"K_MEANS_PARALLEL"和"RANDOM" | String |  | "K_MEANS_PARALLEL" |
| initSteps | k-means++初始化迭代步数 | k-means初始化中心点时迭代的步数 | Integer |  | 2 |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  | 2 |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  | 1.0E-4 |<!-- This is the end of auto-generated parameter info -->

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
inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
kmeans = KMeansTrainBatchOp().setVectorCol("vec").setK(2)
predictBatch = KMeansPredictBatchOp().setPredictionCol("pred")
kmeans.linkFrom(inOp1)
predictBatch.linkFrom(kmeans, inOp1)
[model,predict] = collectToDataframes(kmeans, predictBatch)
print(model)
print(predict)

predictStream = KMeansPredictStreamOp(kmeans).setPredictionCol("pred")
predictStream.linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### 脚本运行结果
##### 模型结果
```
model_id                                         model_info
0         0  {"vectorCol":"\"vec\"","latitudeCol":null,"lon...
1   1048576  {"clusterId":0,"weight":6.0,"vec":{"data":[9.0...
2   2097152  {"clusterId":1,"weight":6.0,"vec":{"data":[0.1...
```

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