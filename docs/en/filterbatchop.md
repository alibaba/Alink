## Description
Filter records in the batch operator.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| clause | Operation clause. | String | ✓ |  |

## Script Example
### Code

```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

data = {
  'f1': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada', 'Nevada'],
  'f2': [2000, 2001, 2002, 2001, 2002, 2003],
  'f3': [1.5, 1.7, 3.6, 2.4, 2.9, 3.2]
}

df_data = pd.DataFrame(data)
schema = 'f1 string, f2 bigint, f3 double'

batch_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
op = FilterBatchOp().setClause("f1='Ohio'")
batch_data = batch_data.link(op)
batch_data.print()

resetEnv()

```

### Result

```
     f1    f2   f3
0  Ohio  2000  1.5
1  Ohio  2001  1.7
2  Ohio  2002  3.6
```
