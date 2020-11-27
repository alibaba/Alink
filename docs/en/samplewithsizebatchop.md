## Description
Sample the input data with given size with or without replacement.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| size | sampling size | Integer | ✓ |  |
| withReplacement | Indicates whether to enable sampling with replacement, default is without replcement | Boolean |  | false |

## Script Example

### Code

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
  ["0,0,0"],
  ["0.1,0.1,0.1"],
  ["0.2,0.2,0.2"],
  ["9,9,9"],
  ["9.1,9.1,9.1"],
  ["9.2,9.2,9.2"]
])

df = pd.DataFrame({"Y": data[:, 0]})

# batch source
inOp = dataframeToOperator(df, schemaStr='Y string', op_type='batch')

sampleOp = SampleWithSizeBatchOp() \
  .setSize(2) \
  .setWithReplacement(False)

inOp.link(sampleOp).print()
resetEnv()

```
### Result
```
|Y|
|---|
|0,0,0|
|0.2,0.2,0.2|
```




