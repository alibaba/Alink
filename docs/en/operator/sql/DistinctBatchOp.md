## Description
Remove duplicated records.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |


## Script Example
### Code

```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

data = {
  'f1': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada', 'Nevada'],
}

df_data = pd.DataFrame(data)
schema = 'f1 string'

batch_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
op = DistinctBatchOp()
batch_data = batch_data.link(op)
batch_data.print()

resetEnv()
```

### Result

```
       f1
0  Nevada
1    Ohio
```
