## Description
Fit a equal width discretizer model: all bins have equal width.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| numBuckets | number of buckets | Integer |  | 2 |
| numBucketsArray | Array of num bucket | Integer[] |  | null |
| leftOpen | left open | Boolean | | true |


## Script Example

### Code

```python
import numpy as np
import pandas as pd
data = np.array([
    [0, 0],
	[8, 8],
	[1, 2],
	[9, 10],
	[3, 1],
	[10, 7]
])
df = pd.DataFrame({"col0": data[:, 0], "col1": data[:, 1]})
inOp = dataframeToOperator(df, schemaStr='col0 long, col1 long', op_type='batch')
inOpStream = dataframeToOperator(df, schemaStr='col0 long, col1 long', op_type='stream')

train = EqualWidthDiscretizerTrainBatchOp().setNumBuckets(2).setSelectedCols(["col0"]).linkFrom(inOp)
EqualWidthDiscretizerPredictBatchOp().setSelectedCols(["col0"]).linkFrom(train, inOp).print()
EqualWidthDiscretizerPredictStreamOp(train).setSelectedCols(["col0"]).linkFrom(inOpStream).print()

StreamOperator.execute()
```

### Result
```
   col0  col1
0     0     0
1     1     8
2     0     2
3     1    10
4     0     1
5     1     7
```

