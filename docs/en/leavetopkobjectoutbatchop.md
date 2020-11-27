## Description
Leave-k-out cross validation.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| rateThreshold | rate threshold | Double |  | -Infinity |
| rateCol | Rating column name | String | ✓ |  |
| groupCol | Name of a grouping column | String | ✓ |  |
| objectCol | Object column name | String | ✓ |  |
| fraction | Proportion of data allocated to right output after splitting | Double |  | 1.0 |
| k | Number of the recommended top objects. | Integer |  | 10 |
| outputCol | Name of the output column | String | ✓ |  |

## Script Example
### Code

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

data = np.array([
    [1, 1, 0.6],
    [2, 2, 0.8],
    [2, 3, 0.6],
    [4, 0, 0.6],
    [6, 4, 0.3],
    [4, 7, 0.4],
    [2, 6, 0.6],
    [4, 5, 0.6],
    [4, 6, 0.3],
    [4, 3, 0.4],
])

df_data = pd.DataFrame({
    "user": data[:, 0],
    "item": data[:, 1],
    "rating": data[:, 2],
})
df_data["user"] = df_data["user"].astype('int')
df_data["item"] = df_data["item"].astype('int')

schema = 'user bigint, item bigint, rating double'
data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
spliter = LeaveTopKObjectOutBatchOp()\
			.setK(2)\
			.setGroupCol("user")\
			.setObjectCol("item")\
			.setOutputCol("label")\
            .setRateCol("rating")
spliter.linkFrom(data).print()
spliter.getSideOutput(0).print()

```

### Results
```
	user	label
0	1	{"rating":"[0.6]","object":"[1]"}
1	2	{"rating":"[0.8,0.6]","object":"[2,3]"}
2	4	{"rating":"[0.6,0.6]","object":"[0,5]"}
3	6	{"rating":"[0.3]","object":"[4]"}
        user	item	rating
  0	2	6	0.6
  1	4	7	0.4
  2	4	3	0.4
  3	4	6	0.3
```
