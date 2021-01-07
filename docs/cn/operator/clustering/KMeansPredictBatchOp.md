## 功能介绍

KMeans 是一个经典的聚类算法。

基本思想是：以空间中k个点为中心进行聚类，对最靠近他们的对象归类。通过迭代的方法，逐次更新各聚类中心的值，直至得到最好的聚类结果。

Alink上KMeans算法括[KMeans]，[KMeans批量预测], [KMeans流式预测]

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |
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



