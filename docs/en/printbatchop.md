## Description
Print batch op to std out.

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



inOp.link(PrintBatchOp())

```

### Results

```
             Y
0        0,0,0
1  0.1,0.1,0.1
2  0.2,0.2,0.2
3        9,9,9
4  9.1,9.1,9.1
5  9.2,9.2,9.2
```
