## Description
shuffle data.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |


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



inOp.link(ShuffleBatchOp()).print()

```

### Results

```
             Y
0  9.1,9.1,9.1
1        0,0,0
2  0.2,0.2,0.2
3        9,9,9
4  9.2,9.2,9.2
5  0.1,0.1,0.1
```
