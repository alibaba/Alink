# 线性SVR流预测 (LinearSvrPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.LinearSvrPredictStreamOp

Python 类名：LinearSvrPredictStreamOp


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
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |



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
            [22.0, 2.4, 2.5]])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')
streamData = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')

colnames = ["f0", "f1"]

lsvr = LinearSvrTrainBatchOp()\
        .setFeatureCols(colnames)\
        .setLabelCol("label")\
        .setC(1.0)\
        .setTau(0.01)

model = batchData.link(lsvr)
predictor = LinearSvrPredictStreamOp(model)\
        .setPredictionCol("pred")

predictor.linkFrom(streamData).print()

StreamOperator.execute()
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






