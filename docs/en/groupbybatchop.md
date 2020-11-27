## Description
Apply the "group by" operation on the input batch operator.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| groupByPredicate | Group by clause. | String | ✓ |  |
| selectClause | Select clause | String | ✓ |  |

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

op = GroupByBatchOp().setGroupByPredicate("f1").setSelectClause("f1,avg(f2) as f2")
batch_data = batch_data.link(op)

batch_data.print()

resetEnv()
```

### Result

```
       f1    f2
0  Nevada  2002
1    Ohio  2001
```
