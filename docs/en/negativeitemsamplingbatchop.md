## Description
Given a dataset of user-item pairs, generate new user-item pairs and add them to original dataset.
 For the user in each user-item pair, a "SAMPLING_FACTOR" number of items are sampled.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| samplingFactor |  | Integer |  | 3 |

## Script Example
### Code

```python
from pyalink.alink import *
import pandas as pd
import numpy as np


data = np.array([
    [1, 1],
    [2, 2],
    [2, 3],
    [4, 1],
    [4, 2],
    [4, 3],
])

df_data = pd.DataFrame({
    "user": data[:, 0],
    "item": data[:, 1]
})
df_data["user"] = df_data["user"].astype('int')
df_data["item"] = df_data["item"].astype('int')

schema = 'user bigint, item bigint'
data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

NegativeItemSamplingBatchOp().linkFrom(data).print()
```

### Results
```
	user	item	label
0	2	1	0
1	1	1	1
2	2	1	0
3	1	2	0
4	2	1	0
5	2	2	1
6	2	1	0
7	2	1	0
8	4	2	1
9	4	3	1
10	1	3	0
11	2	1	0
12	2	3	1
13	4	1	1
14	1	2	0
```
