## Description
The random forest use the bagging to prevent the overfitting.

 In the operator, we implement three type of decision tree to
 increase diversity of the forest.
 <ul>
 <tr>id3</tr>
 <tr>cart</tr>
 <tr>c4.5</tr>
 </ul>
 and the criteria is
 <ul>
 <tr>information</tr>
 <tr>gini</tr>
 <tr>information ratio</tr>
 <tr>mse</tr>
 </ul>

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| numThreads | Thread number of operator. | Integer |  | 1 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

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
    DecisionTreeTrainBatchOp()
    .setLabelCol('label')
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])
    .linkFrom(batchSource())
)

predictBatchOp = (
    DecisionTreePredictBatchOp()
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
    DecisionTreePredictStreamOp(
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


