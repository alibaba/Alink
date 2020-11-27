## Description
Filter records in the stream operator.

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
  'f1': ['changjiang', 'huanghe', 'zhujiang', 'changjiang', 'huanghe', 'zhujiang'],
  'f2': [2000, 2001, 2002, 2001, 2002, 2003],
  'f3': [1.5, 1.7, 3.6, 2.4, 2.9, 3.2]
}
df_data = pd.DataFrame(data)
schema = 'f1 string, f2 bigint, f3 double'
stream_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

op = FilterStreamOp().setClause("f1='zhujiang'")
stream_data = stream_data.link(op)

stream_data.print()
StreamOperator.execute()
resetEnv()

```

### Results

```
['f1', 'f2', 'f3']
['zhujiang', 2002, 3.6]
['zhujiang', 2003, 3.2]
```
