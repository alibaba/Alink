## Description
The batch operator that predict the data using the quantile discretizer model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCols | Names of the output columns | String[] |  | null |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "KEEP" |
| encode | encode type: INDEX, VECTOR, ASSEMBLED_VECTOR. | String |  | "INDEX" |
| dropLast | drop last | Boolean |  | true |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example

### Code

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        ["a", 1, 1, 2.0, True],
        ["c", 1, 2, -3.0, True],
        ["a", 2, 2, 2.0, False],
        ["c", 0, 0, 0.0, False]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f_string": data[:, 0],
        "f_long": data[:, 1],
        "f_int": data[:, 2],
        "f_double": data[:, 3],
        "f_boolean": data[:, 4]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
    f_double double, 
    f_boolean boolean
    ''',
        op_type='batch'
    )


def streamSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
    f_double double, 
    f_boolean boolean
    ''',
        op_type='stream'
    )


trainOp = (
    QuantileDiscretizerTrainBatchOp()
    .setSelectedCols(['f_double'])
    .setNumBuckets(8)
    .linkFrom(batchSource())
)

predictBatchOp = (
    QuantileDiscretizerPredictBatchOp()
    .setSelectedCols(['f_double'])
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
    QuantileDiscretizerPredictStreamOp(
        trainOp
    )
    .setSelectedCols(['f_double'])
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

### Result
Batch prediction
```
f_string  f_long  f_int  f_double  f_boolean
0        a       1      1         2       True
1        c       1      2         0       True
2        a       2      2         2      False
3        c       0      0         1      False
```
Stream Prediction
```
f_string    f_long  f_int   f_double    f_boolean
0   a   1   1   2   True
1   a   2   2   2   False
2   c   1   2   0   True
3   c   0   0   1   False
```
