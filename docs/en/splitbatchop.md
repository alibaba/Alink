## Description
Split a dataset into two parts.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| fraction | Proportion of data allocated to left output after splitting | Double | ✓ |  |
| randomSeed | Random seed, it should be positive integer | Integer |  | null |

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

spliter = SplitBatchOp().setFraction(0.5)
spliter.linkFrom(batch_data)

spliter.print()

resetEnv()
```

### Results
```
       f1    f2   f3
0    Ohio  2001  1.7
1    Ohio  2002  3.6
2  Nevada  2002  2.9
```
