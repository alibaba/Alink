## Description
Fit a c45 model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| maxDepth | depth of the tree | Integer |  | 2147483647 |
| minSamplesPerLeaf | Minimal number of sample in one leaf. | Integer |  | 2 |
| createTreeMode | series or parallel | String |  | "series" |
| maxBins | MAX number of bins for continuous feature | Integer |  | 128 |
| maxMemoryInMB | max memory usage in tree histogram aggregate. | Integer |  | 64 |
| featureCols | Names of the feature columns used for training in the input table | String[] | ✓ |  |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| categoricalCols | Names of the categorical columns used for training in the input table | String[] |  |  |
| weightCol | Name of the column indicating weight | String |  | null |
| maxLeaves | max leaves of tree | Integer |  | 2147483647 |
| minSampleRatioPerChild | Minimal value of: (num of samples in child)/(num of samples in its parent). | Double |  | 0.0 |
| minInfoGain | minimum info gain when performing split | Double |  | 0.0 |

## Script Example

#### Code

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
    C45TrainBatchOp()
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
    .linkFrom(batchSource())
)

predictBatchOp = (
    C45PredictBatchOp()
    .setPredictionDetailCol('pred_detail')
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
    C45PredictStreamOp(
        trainOp
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

#### Result
Batch prediction
```
    f0 f1  f2  f3  label  pred        pred_detail
0  1.0  A   0   0      0     0  {"0":1.0,"1":0.0}
1  2.0  B   1   1      0     0  {"0":1.0,"1":0.0}
2  3.0  C   2   2      1     1  {"0":0.0,"1":1.0}
3  4.0  D   3   3      1     1  {"0":0.0,"1":1.0}
```
Stream Prediction
```
f0	f1	f2	f3	label	pred	pred_detail
0	2.0	B	1	1	0	0	{"0":1.0,"1":0.0}
1	4.0	D	3	3	1	1	{"0":0.0,"1":1.0}
2	1.0	A	0	0	0	0	{"0":1.0,"1":0.0}
3	3.0	C	2	2	1	1	{"0":0.0,"1":1.0}
```

## 备注

- 该组件支持在可视化大屏直接查看模型信息


