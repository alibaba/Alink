# Ridge回归算法

## 功能介绍
* Ridge回归是一个回归算法
* Ridge回归组件支持稀疏、稠密两种数据格式
* Ridge回归组件支持带样本权重的训练


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 脚本示例
### 脚本代码
``` python
import numpy as np
import pandas as pd
from pyalink.alink import *
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
colnames = ["f0","f1"]
ridge = RidgeRegTrainBatchOp().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ridge)

predictor = RidgeRegPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```
### 运行结果
f0 | f1 | label | pred
---|----|-------|-----
 2 |  1     | 1 | 0.830304
   3 |  2    |  1 | 1.377312
   4 |  3    |  2 | 1.924320
   2 |  4    |  1 | 1.159119
   2 |  2    |  1 | 0.939909
   4 |  3    |  2 | 1.924320
   1 |  2    |  1 | 0.502506
   5 |  3    |  3 | 2.361724



