# Gbrank预测 (基于gbdt的learning to rank模型）

## 功能介绍

- 支持连续特征和离散特征

- 支持数据采样和特征采样

- 需要指定分组列，模型会在每一组内根据label列拟合排序结果。和原始论文一样，label列表示“相关度”，越高越好，在计算得分时，会使用2^label -1 来计算。例如可以使用0,1,2,3,....来依次表示很不相关、有点相关、比较相关、相关等等

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |





### 参数建议

对于训练效果来说，比较重要的参数是 树的棵树+学习率、叶子节点最小样本数、单颗树最大深度、特征采样比例。

单个离散特征的取值种类数不能超过256，否则会出错。



## 脚本示例

#### 脚本代码

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        [2, 1, 0, 0],
        [3, 2, 0, 2],
        [4, 3, 0, 1],
        [2, 4, 0, 0],
        [2, 2, 1, 0],
        [4, 3, 1, 2],
        [1, 2, 1, 1],
        [5, 3, 1, 0]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f0": data[:, 0],
        "f1": data[:, 1],
        "group": data[:, 2],
        "label": data[:, 3]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 double,
    group long, 
    label double
    ''',
        op_type='batch'
    )


def streamSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 double,
    group long, 
    label double
    ''',
        op_type='stream'
    )

trainOp = (
    GBRankBatchOp()
    .setFeatureCols(['f0', 'f1'])
    .setLabelCol("label")
    .setGroupCol("group")
    .linkFrom(batchSource())
)

predictBatchOp = (
    GBRankPredictBatchOp()
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
    GBRankPredictStreamOp(
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

#### 脚本运行结果

```
	f0	f1	group	label	pred
0	2.0	1.0	0	0.0	-10.667265
1	3.0	2.0	0	2.0	12.336884
2	4.0	3.0	0	1.0	4.926796
3	2.0	4.0	0	0.0	-10.816632
4	2.0	2.0	1	0.0	-8.938445
5	4.0	3.0	1	2.0	4.926796
6	1.0	2.0	1	1.0	-1.877382
7	5.0	3.0	1	0.0	-11.284907
```
