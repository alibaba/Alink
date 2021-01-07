## Description
Rename the fields of a stream operator.

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

stream_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')
op = AsStreamOp().setClause("ff1,ff2,ff3")
stream_data = stream_data.link(op)
stream_data.print()
StreamOperator.execute()

resetEnv()

```

### Results
```
['ff1', 'ff2', 'ff3']
['Ohio', 2000, 1.5]
['Ohio', 2001, 1.7]
['Ohio', 2002, 3.6]
['Nevada', 2001, 2.4]
['Nevada', 2002, 2.9]
['Nevada', 2003, 3.2]
```
