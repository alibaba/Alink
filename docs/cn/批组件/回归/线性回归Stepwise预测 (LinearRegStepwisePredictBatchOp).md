# 线性回归Stepwise预测 (LinearRegStepwisePredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.regression.LinearRegStepwisePredictBatchOp

Python 类名：LinearRegStepwisePredictBatchOp


## 功能介绍
* Stepwise回归是一个回归算法
* Stepwise回归组件仅支持稠密数据格式


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
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

batchData = BatchOperator.fromDataframe(df, schemaStr='y double, x1 double, x2 double')

lrs = LinearRegStepwiseTrainBatchOp()\
    .setFeatureCols(["x1", "x2"])\
    .setLabelCol("y")\
    .setMethod("Forward")

model = batchData.link(lrs)

predictor = LinearRegStepwisePredictBatchOp()\
    .setPredictionCol("pred")

predictor.linkFrom(model, batchData).print()
```
### 运行结果
 y | x1 | x2 | pred
---|----|----|-----
16.3|1.1|1.1|16.380060195635785
16.8|1.4|1.5|17.698344620015032
19.2|1.7|1.8|19.01662904439428
18.0|1.7|1.7|19.01662904439428
19.5|1.8|1.9|19.456057185854025
20.9|1.8|1.8|19.456057185854025
21.1|1.9|1.8|19.89548532731377
20.9|2.0|2.1|20.33491346877352
20.3|2.3|2.4|21.653197893152765
22.0|2.4|2.5|22.092626034612515



