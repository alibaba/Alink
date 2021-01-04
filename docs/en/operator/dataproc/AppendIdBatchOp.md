## Description
Append an id column to BatchOperator. the id can be DENSE or UNIQUE

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| appendType | append type. DENSE or UNIQUE | String |  | "DENSE" |
| idCol | Id column name | String |  | "append_id" |

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


(
    AppendIdBatchOp()
    .setIdCol("append_id")
    .linkFrom(batchSource())
    .print()
)
```

#### Result

```
    f0 f1  f2  f3  label  append_id
0  1.0  A   0   0      0          0
1  2.0  B   1   1      0          1
2  3.0  C   2   2      1          2
3  4.0  D   3   3      1          3
```

