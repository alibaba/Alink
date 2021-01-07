# 逻辑回归流预测

## 功能介绍
逻辑回归预测组件，读取数据和模型，对数据流进行预测

## 算法参数


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



## 脚本示例
#### 运行脚本
```
import numpy as np
import pandas as pd
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 2]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

streamData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='stream')
batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
dataTest = streamData
colnames = ["f0","f1"]
lr = LogisticRegressionTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = LogisticRegressionPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(dataTest).print()
StreamOperator.execute()
```
#### 运行结果
f0 | f1 | label | pred 
---|----|-------|-----
2|1|1|1
3|2|1|1
4|3|2|2
2|4|1|1
2|2|1|1
4|3|2|2
1|2|1|1
5|3|2|2




