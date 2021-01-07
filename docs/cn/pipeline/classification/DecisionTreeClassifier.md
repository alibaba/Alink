# 决策树

## 功能介绍

- 决策树支持多种树模型

- id3，cart，c4.5

- 支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| treeType | 模型中树的类型 | 模型中树的类型，三种选项可选：树为一种方式gini, infoGain, infoGainRatio | String |  | "GINI" |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  | 2147483647 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  | 2 |
| createTreeMode | 创建树的模式。 | series表示每个单机创建单颗树，parallel表示并行创建单颗树。 | String |  | "series" |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  | 128 |
| maxMemoryInMB | 树模型中用来加和统计量的最大内存使用数 | 树模型中用来加和统计量的最大内存使用数 | Integer |  | 64 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  | 2147483647 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  | 0.0 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  | 0.0 |

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


(
    DecisionTreeClassifier()
    .setPredictionDetailCol('pred_detail')
    .setPredictionCol('pred')
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
    .fit(batchSource())
    .transform(batchSource())
    .print()
)

(
    DecisionTreeClassifier()
    .setPredictionDetailCol('pred_detail')
    .setPredictionCol('pred')
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
    .fit(batchSource())
    .transform(streamSource())
    .print()
)

StreamOperator.execute()
```

#### 脚本结果
批预测结果
```
    f0 f1  f2  f3  label  pred        pred_detail
0  1.0  A   0   0      0     0  {"0":1.0,"1":0.0}
1  2.0  B   1   1      0     0  {"0":1.0,"1":0.0}
2  3.0  C   2   2      1     1  {"0":0.0,"1":1.0}
3  4.0  D   3   3      1     1  {"0":0.0,"1":1.0}
```
流预测结果
```
f0	f1	f2	f3	label	pred	pred_detail
0	2.0	B	1	1	0	0	{"0":1.0,"1":0.0}
1	4.0	D	3	3	1	1	{"0":0.0,"1":1.0}
2	1.0	A	0	0	0	0	{"0":1.0,"1":0.0}
3	3.0	C	2	2	1	1	{"0":0.0,"1":1.0}
```
