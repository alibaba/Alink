## Description
StratifiedSample with given size with or without replacement.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| strataCol | strata col name. | String | ✓ |  |
| strataSize | strata size. | Integer |  | -1 |
| strataSizes | strata sizes. a:10,b:30 | String | ✓ |  |
| withReplacement | Indicates whether to enable sampling with replacement, default is without replcement | Boolean |  | false |

## Script Example

### Code

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

data = np.array([
        ['a',0.0,0.0],
        ['a',0.2,0.1],
        ['b',0.2,0.8],
        ['b',9.5,9.7],
        ['b',9.1,9.6],
        ['b',9.3,9.9]
    ])

df_data = pd.DataFrame({
    "x1": data[:, 0],
    "x2": data[:, 1],
    "x3": data[:, 2]
})

batchData = dataframeToOperator(df_data, schemaStr='x1 string, x2 double, x3 double', op_type='batch')
sampleOp = StratifiedSampleWithSizeBatchOp() \
       .setStrataCol("x1") \
       .setStrataSizes("a:1,b:2")

batchData.link(sampleOp).print()
```

### Results

```
  x1   x2   x3
0  a  0.0  0.0
1  b  9.1  9.6
2  b  0.2  0.8
```
