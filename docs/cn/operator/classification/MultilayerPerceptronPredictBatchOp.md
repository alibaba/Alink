# 多层感知机预测

## 功能介绍
基于多层感知机模型，进行分类预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例
### 脚本代码
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = {
  'f1': np.random.rand(12),
  'f2': np.random.rand(12),
  'label': np.random.randint(low=0, high=3, size=12)
}

df_data = pd.DataFrame(data)
schema = 'f1 double, f2 double, label bigint'
train_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
test_data = train_data

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["f1", "f2"]) \
  .setLabelCol("label") \
  .setLayers([2, 8, 3]) \
  .setMaxIter(10)

model = mlpc.linkFrom(train_data)

predictor = MultilayerPerceptronPredictBatchOp()\
  .setPredictionCol('p')

predictor.linkFrom(model, test_data).print()

resetEnv()

```

### 脚本运行结果

```
          f1        f2  label  p
0   0.398737  0.088554      2  2
1   0.129992  0.025044      1  2
2   0.569760  0.359337      2  2
3   0.672308  0.771075      0  2
4   0.546880  0.436831      2  2
5   0.096233  0.962563      2  2
6   0.567075  0.968310      1  2
7   0.706059  0.998861      0  2
8   0.113452  0.231905      2  2
9   0.686377  0.417600      1  2
10  0.861515  0.288288      2  2
11  0.719469  0.736065      1  2

```
