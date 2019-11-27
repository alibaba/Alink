# gbdt回归

## 功能介绍

- gbdt(Gradient Boosting Decision Trees)回归，是经典的基于boosting的有监督学习模型，可以用来解决回归问题

- 支持连续特征和离散特征

- 支持数据采样和特征采样

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| learningRate | 学习率 | 学习率（默认为0.3） | Double |  | 0.3 |
| minSumHessianPerLeaf | 叶子节点最小Hessian值 | 叶子节点最小Hessian值（默认为0） | Double |  | 0.0 |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  | 100 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  | 100 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  | 6 |
| subsamplingRatio | 每棵树的样本采样比例或采样行数 | 每棵树的样本采样比例或采样行数，行数上限100w行 | Double |  | 1.0 |
| featureSubsamplingRatio | 每棵树特征采样的比例 | 每棵树特征采样的比例，范围为(0, 1]。 | Double |  | 1.0 |
| groupCol | 分组单列名 | 分组单列名，可选 | String |  | null |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  | 128 |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| categoricalCols | 离散特征列名 | 可选，默认选择String类型和Boolean类型作为离散特征，如果没有则为空 | String[] |  |  |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  | 2147483647 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  | 0.0 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  | 0.0 |




### 参数建议
对于训练效果来说，比较重要的参数是 树的棵树+学习率、叶子节点最小样本数、单颗树最大深度、特征采样比例。

单个离散特征的取值种类数不能超过256，否则会出错。

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
    GbdtRegTrainBatchOp()
    .setLearningRate(1.0)
    .setNumTrees(3)
    .setMinSamplesPerLeaf(1)
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
)

predictBatchOp = (
    GbdtRegPredictBatchOp()
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
    GbdtRegPredictStreamOp(
        batchSource().link(trainOp)
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
批预测结果
```
    f0 f1  f2  f3  label  pred
0  1.0  A   0   0      0   0.0
1  2.0  B   1   1      0   0.0
2  3.0  C   2   2      1   1.0
3  4.0  D   3   3      1   1.0
```
流预测结果
```
	f0	f1	f2	f3	label	pred
0	1.0	A	0	0	0	0.0
1	3.0	C	2	2	1	1.0
2	2.0	B	1	1	0	0.0
3	4.0	D	3	3	1	1.0
```


