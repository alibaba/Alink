## 功能介绍
二分k均值算法是k-means聚类算法的一个变体，主要是为了改进k-means算法随机选择初始质心的随机性造成聚类结果不确定性的问题.

Alink上算法括[二分K均值聚类训练]，[二分K均值聚类预测], [二分K均值聚类流式预测]

## 参数说明
#### 训练
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
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
inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
kmeans = BisectingKMeansTrainBatchOp().setVectorCol("vec").setK(2)
predictBatch = BisectingKMeansPredictBatchOp().setPredictionCol("pred")
kmeans.linkFrom(inOp1)
predictBatch.linkFrom(kmeans, inOp1)
[model,predict] = collectToDataframes(kmeans, predictBatch)
print(model)
print(predict)

predictStream = BisectingKMeansPredictStreamOp(kmeans).setPredictionCol("pred")
predictStream.linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### 脚本运行结果
##### 模型结果
```
rowId   model_id                                         model_info
0         0  {"vectorCol":"\"vec\"","distanceType":"\"EUCLI...
1   1048576  {"clusterId":1,"size":6,"center":{"data":[4.6,...
2   2097152  {"clusterId":2,"size":3,"center":{"data":[0.1,...
3   3145728  {"clusterId":3,"size":3,"center":{"data":[9.1,...
```

##### 预测结果
```
rowId   id          vec  pred
0   0        0 0 0     0
1   1  0.1,0.1,0.1     0
2   2  0.2,0.2,0.2     0
3   3        9 9 9     1
4   4  9.1 9.1 9.1     1
5   5  9.2 9.2 9.2     1
```





