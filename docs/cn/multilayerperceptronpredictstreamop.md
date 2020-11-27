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
test_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["f1", "f2"]) \
  .setLabelCol("label") \
  .setLayers([2, 8, 3]) \
  .setMaxIter(10)

model = mlpc.linkFrom(train_data)

predictor = MultilayerPerceptronPredictStreamOp(model)\
  .setPredictionCol('p')

predictor.linkFrom(test_data).print()
StreamOperator.execute()

resetEnv()

```

### 脚本运行结果

```
['f1', 'f2', 'label', 'p']
[0.32157748818958753, 0.49787124043277453, 0, 1]
[0.9988915548832964, 0.8030743157012032, 2, 0]
[0.33727230130595953, 0.81275648997722, 2, 1]
[0.7680458610932143, 0.9917353120756056, 1, 1]
[0.47095782127772334, 0.39421675200119843, 0, 0]
[0.8019966973978703, 0.13171211349239198, 2, 2]
[0.43242357294095524, 0.5696606829395613, 1, 1]
[0.7475103692009955, 0.7353101866795212, 0, 1]
[0.24565789099075186, 0.4938085074750497, 1, 1]
[0.08361045521633703, 0.5737040509691121, 1, 1]
[0.22910529416272918, 0.5867291811070908, 0, 1]
[0.16013950289548107, 0.8000181963308167, 1, 1]
```
