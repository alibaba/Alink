## Description
Stratified sample with given ratios without replacement.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| strataCol | strata col name. | String | ✓ |  |
| strataRatio | strata ratio. | Double |  | -1.0 |
| strataRatios | strata ratios. a:0.1,b:0.3 | String | ✓ |  |

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

streamData = dataframeToOperator(df_data, schemaStr='x1 string, x2 double, x3 double', op_type='stream')
sampleStreamOp = StratifiedSampleStreamOp()\
       .setStrataCol("x1")\
       .setStrataRatios("a:0.5,b:0.5")

sampleStreamOp.linkFrom(streamData).print()

StreamOperator.execute()
```
### Results

x1|x2|x3
---|---|---
b|9.3|9.9
a|0.0|0.0
b|0.2|0.8



