## Description
Split a stream data into two parts.

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

stream_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

spliter = SplitStreamOp().setFraction(0.5)
spliter.linkFrom(stream_data)

spliter.print()
StreamOperator.execute()
resetEnv()
```

### Results
```
['f1', 'f2', 'f3']
['Ohio', 2000, 1.5]
['Ohio', 2001, 1.7]
['Ohio', 2002, 3.6]
['Nevada', 2001, 2.4]
['Nevada', 2003, 3.2]
```
