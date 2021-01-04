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
   [1,'{"rating":"[0.6]","object":"[1]"}'],
   [2,'{"rating":"[0.8,0.6]","object":"[2,3]"}'],
   [3,'{"rating":"[0.6,0.3,0.4]","object":"[1,2,3]"}']
])

df_data = pd.DataFrame({
    "user": data[:, 0],
    "rec": data[:, 1]
})
df_data["user"] = df_data["user"].astype('int')
df_data["rec"] = df_data["rec"].astype('str')

schema = 'user bigint, rec string'
jsonData = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

recList = FlattenKObjectStreamOp()\
			.setSelectedCol("rec")\
			.setOutputColTypes(["long","double"])\
			.setReservedCols(["user"])\
			.setOutputCols(["object", "rating"])\
			.linkFrom(jsonData).print();
StreamOperator.execute();
```

### Results
```
	user	object	rating
0	1	1	0.6
1	2	2	0.8
2	2	3	0.6
3	3	1	0.6
4	3	2	0.3
5	3	3	0.4
```
