## Description
Transform json format recommendation to table format.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCols | Names of the output columns | String[] | ✓ |  |
| outputColTypes | Types of the output columns | String[] |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

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
    [4, 1, 0.6],
    [4, 2, 0.3],
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
jsonData = Zipped2KObjectBatchOp()\
			.setGroupCol("user")\
			.setObjectCol("item")\
			.setInfoCols(["rating"])\
			.setOutputCol("recomm")\
			.linkFrom(data)\
			.lazyPrint(-1);
recList = FlattenKObjectBatchOp()\
			.setSelectedCol("recomm")\
			.setOutputColTypes(["long","double"])\
			.setReservedCols(["user"])\
			.setOutputCols(["object", "rating"])\
			.linkFrom(jsonData)\
			.lazyPrint(-1);
BatchOperator.execute();
```

### Results
```
        user	recomm
0	1	{"rating":"[0.6]","object":"[1]"}
1	2	{"rating":"[0.8,0.6]","object":"[2,3]"}
2	4	{"rating":"[0.6,0.3,0.4]","object":"[1,2,3]"}

        user	object	rating
0	1	1	0.6
1	2	2	0.8
2	2	3	0.6
3	4	1	0.6
4	4	2	0.3
5	4	3	0.4
```
