## Description
BatchOperator to select first n records.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| size | sampling size | Integer | ✓ |  |

## Script Example

### Code

```python
import numpy as np
import pandas as pd

data = data = np.array([
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

sampleOp = FirstNBatchOp()\
        .setSize(2)

inOp.link(sampleOp).print()

```

### Results

```
             Y
0        0,0,0
1  0.1,0.1,0.1
```
