## 功能介绍

KNN (K Nearest Neighbor）是一种分类算法。
KNN算法的核心思想是如果一个样本在特征空间中的k个最相邻的样本中的大多数属于某一个类别;
则该样本也属于这个类别，并具有这个类别上样本的特性。

KNN的训练与一般机器学习模型的训练过程不同：在KNN训练中我们只进行一些字典表的预处理，而在预测过程中才会进行计算预测每个数据点的类别。
因此，KNN的训练和预测通常同时使用，一般不单独使用。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| distanceType | 距离度量方式 | 聚类使用的距离类型 | String |  | "EUCLIDEAN" |


## 脚本示例
#### 脚本代码
```
from pyalink.alink import *
import numpy as np
import pandas as pd

useLocalEnv(2)

data = np.array([
    [1, 0, 0],
    [2, 8, 8],
    [1, 1, 2],
    [2, 9, 10],
    [1, 3, 1],
    [2, 10, 7]
])
df = pd.DataFrame({"label": data[:, 0], "f0": data[:, 1], "f1": data[:, 2]})

dataSourceOp = dataframeToOperator(df, schemaStr="label int, f0 int, f1 int", op_type='batch')
trainOp = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
predictOp = KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp)
predictOp.print()
```

#### 脚本运行结果
```
   label  f0  f1  pred
0      1   0   0     1
1      2   8   8     2
2      1   1   2     1
3      2   9  10     2
4      1   3   1     1
5      2  10   7     2
```
