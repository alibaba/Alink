## 功能介绍

- gbdt(Gradient Boosting Decision Trees)二分类，是经典的基于boosting的有监督学习模型，可以用来解决二分类问题

- 支持连续特征和离散特征

- 支持数据采样和特征采样

- 目标分类必须是两个

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例

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
    GbdtTrainBatchOp()
    .setLearningRate(1.0)
    .setNumTrees(3)
    .setMinSamplesPerLeaf(1)
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
)

predictBatchOp = (
    GbdtPredictBatchOp()
    .setPredictionDetailCol('pred_detail')
    .setPredictionCol('pred')
)

(
    predictBatchOp
    .linkFrom(
        batchSource().link(trainOp),
        batchSource()
    )
    .print()
)

predictStreamOp = (
    GbdtPredictStreamOp(
        batchSource().link(trainOp)
    )
    .setPredictionDetailCol('pred_detail')
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
	f0	f1	f2	f3	label	pred	pred_detail
0	1.0	A	0	0	0	0	{"0":0.9849144951094335,"1":0.015085504890566462}
1	3.0	C	2	2	1	1	{"0":0.01508550489056637,"1":0.9849144951094336}
2	2.0	B	1	1	0	0	{"0":0.9849144951094335,"1":0.015085504890566462}
3	4.0	D	3	3	1	1	{"0":0.01508550489056637,"1":0.9849144951094336}
```
