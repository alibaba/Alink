## Description
Weighted sampling with given ratio with/without replacement.
 The probability of being chosen for data point {i} is: weight_{i} / \sum_{j=1}^{n} weight_{j}, where weight_{i} is
 the weight of data point i.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| weightCol | Name of the column indicating weight | String | ✓ |  |
| ratio | sampling ratio, it should be in range of [0, 1] | Double | ✓ |  |
| withReplacement | Indicates whether to enable sampling with replacement, default is without replcement | Boolean |  | false |

## Script Example

### Code
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    ["a", 1.3, 1.1],
    ["b", 2.5, 0.9],
    ["c", 100.2, -0.01],
    ["d", 99.9, 100.9],
    ["e", 1.4, 1.1],
    ["f", 2.2, 0.9],
    ["g", 100.9, -0.01],
    ["j", 99.5, 100.9],
])

df = pd.DataFrame({"id": data[:, 0], "weight": data[:, 1], "value": data[:, 2]})

# batch source
inOp = dataframeToOperator(df, schemaStr='id string, weight double, value double', op_type='batch')
sampleOp = WeightSampleBatchOp() \
  .setWeightCol("weight") \
  .setRatio(0.5) \
  .setWithReplacement(False)

inOp.link(sampleOp).print()
```
### Result
```
  id  weight   value
0  g   100.9   -0.01
1  d    99.9  100.90
2  c   100.2   -0.01
3  j    99.5  100.90
```








