## Description
Transform the Table to SourceStreamOp.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |


## Script Example
### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp.getOutputTable()
TableSourceStreamOp(inOp.getOutputTable()).print()
StreamOperator.execute()
```
### Result
   id    vec
  0  0 0 0
  1  1 1 1
  2  2 2 2


