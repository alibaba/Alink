# Fm 回归预测组件

## 功能介绍

* 使用 Fm 回归模型对数据进行预测
* 算法支持稀疏、稠密两种数据格式

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 脚本示例
#### 运行脚本
```python
import numpy as np
import pandas as pd
data = np.array([
    ["1:1.1 3:2.0", 1.0],
    ["2:2.1 10:3.1", 1.0],
    ["1:1.2 5:3.2", 0.0],
    ["3:1.2 7:4.2", 0.0]])
df = pd.DataFrame({"kv": data[:, 0], 
                   "label": data[:, 1]})

input = dataframeToOperator(df, schemaStr='kv string, label double', op_type='batch')
# load data
dataTest = input
fm = FmRegressorTrainBatchOp().setVectorCol("kv").setLabelCol("label")
model = input.link(fm)

predictor = FmRegressorPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```
#### 运行结果
kv	| label	| pred
---|----|-------
1:1.1 3:2.0|1.0|0.473600
2:2.1 10:3.1|1.0|0.755115
1:1.2 5:3.2|0.0|0.005875
3:1.2 7:4.2|0.0|0.004641





