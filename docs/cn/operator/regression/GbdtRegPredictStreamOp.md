# gbdt回归

## 功能介绍

- gbdt(Gradient Boosting Decision Trees)回归，是经典的基于boosting的有监督学习模型，可以用来解决回归问题

- 支持连续特征和离散特征

- 支持数据采样和特征采样

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |



## 脚本示例

#### 脚本代码

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        [1.0, "A", 0, 0, 0],
        [2.0, "B", 1, 1, 0],
        [3.0, "C", 2, 2, 1],
        [4.0, "D", 3, 3, 1]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f0": data[:, 0],
        "f1": data[:, 1],
        "f2": data[:, 2],
        "f3": data[:, 3],
        "label": data[:, 4]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 string, 
    f2 int, 
    f3 int, 
    label int
    ''',
        op_type='batch'
    )


def streamSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 string, 
    f2 int, 
    f3 int, 
    label int
    ''',
        op_type='stream'
    )


trainOp = (
    GbdtRegTrainBatchOp()
    .setLearningRate(1.0)
    .setNumTrees(3)
    .setMinSamplesPerLeaf(1)
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
    .linkFrom(batchSource())
)

predictBatchOp = (
    GbdtRegPredictBatchOp()
    .setPredictionCol('pred')
)

(
    predictBatchOp
    .linkFrom(
        trainOp,
        batchSource()
    )
    .print()
)

predictStreamOp = (
    GbdtRegPredictStreamOp(
        trainOp
    )
    .setPredictionCol('pred')
)

(
    predictStreamOp
    .linkFrom(
        streamSource()
    )
    .print()
)

StreamOperator.execute()
```
#### 脚本结果
流预测结果
```
	f0	f1	f2	f3	label	pred
0	1.0	A	0	0	0	0.0
1	3.0	C	2	2	1	1.0
2	2.0	B	1	1	0	0.0
3	4.0	D	3	3	1	1.0
```



- 备注：

1. 该组件支持在可视化大屏直接查看模型信息，参见 [模型类组件可视化](https://yuque.antfin-inc.com/pai-user/manual/mqb0xh)。




