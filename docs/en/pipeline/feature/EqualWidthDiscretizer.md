## Description
EqualWidth discretizer keeps every interval the same width, output the interval
 as model, and can transform a new data using the model.
 The output is the index of the interval.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| numBuckets | number of buckets | Integer |  | 2 |
| numBucketsArray | Array of num bucket | Integer[] |  | null |
| leftOpen | indicating if the intervals should be opened on the left. | Boolean |  | true |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCols | Names of the output columns | String[] |  | null |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "KEEP" |
| encode | encode type: INDEX, VECTOR, ASSEMBLED_VECTOR. | String |  | "INDEX" |
| dropLast | drop last | Boolean |  | true |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example

### Code

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        ["a", 1L, 1.1],     
        ["b", -2L, 0.9],    
        ["c", 100L, -0.01], 
        ["d", -99L, 100.9], 
        ["a", 1L, 1.1],     
        ["b", -2L, 0.9],    
        ["c", 100L, -0.01], 
        ["d", -99L, 100.9] 
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f_string": data[:, 0],
        "f_long": data[:, 1],
        "f_int": data[:, 2],
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
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
    ''',
        op_type='stream'
    )


(
    EqualWidthDiscretizer()
    .setSelectedCols(['f_long', 'f_double'])
    .setNumBuckets(5)
    .linkFrom(batchSource())
    .print()
)

    EqualWidthDiscretizer()
    .setSelectedCols(['f_long', 'f_double'])
    .setNumBuckets(5)
    .linkFrom(streamSource())
    .print()
)

StreamOperator.execute()
```

### Result
Batch prediction
```
f_string    f_long  f_int
0   a   2   0
1   b   2   0
2   c   4   0
3   d   0   4
4   a   2   0
5   b   2   0
6   c   4   0
7   d   0   4
```
Stream Prediction
```
f_string    f_long  f_int
0   a   2   0
1   b   2   0
2   c   4   0
3   d   0   4
4   a   2   0
5   b   2   0
6   c   4   0
7   d   0   4
```
