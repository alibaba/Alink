# 线性SVR预测 (LinearSvrPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.LinearSvrPredictBatchOp

Python 类名：LinearSvrPredictBatchOp


## 功能介绍
* 线性SVR是一个回归算法
* 线性SVR组件支持稀疏、稠密两种数据格式
* 线性SVR组件支持带样本权重的训练


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [16.3, 1.1, 1.1],
    [16.8, 1.4, 1.5],
    [19.2, 1.7, 1.8],
    [18.0, 1.7, 1.7],
    [19.5, 1.8, 1.9],
    [20.9, 1.8, 1.8],
    [21.1, 1.9, 1.8],
    [20.9, 2.0, 2.1],
    [20.3, 2.3, 2.4],
    [22.0, 2.4, 2.5]
])

batchSource = BatchOperator.fromDataframe(df, schemaStr=' y double, x1 double, x2 double')

lsvr = LinearSvrTrainBatchOp()\
    .setFeatureCols(["x1", "x2"])\
    .setLabelCol("y")\
    .setC(1.0)\
    .setTau(0.01)

model = batchSource.link(lsvr)

predictor = LinearSvrPredictBatchOp()\
    .setPredictionCol("pred")

predictor.linkFrom(model, batchSource).print()
```
### 运行结果
 y | x1 | x2 | pred
---|----|----|-----
16.3|1.1|1.1|16.48073043727051
16.8|1.4|1.5|17.236649847389877
19.2|1.7|1.8|18.651637270539197
18.0|1.7|1.7|19.31070528356914
19.5|1.8|1.9|19.123299744922306
20.9|1.8|1.8|19.78236775795225
21.1|1.9|1.8|20.913098245365298
20.9|2.0|2.1|20.066624693688514
20.3|2.3|2.4|21.481612116837834
22.0|2.4|2.5|21.953274591220936



