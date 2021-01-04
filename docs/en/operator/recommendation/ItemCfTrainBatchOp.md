## Description
A model that ranks an item according to its calc to other items observed for the user in question.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| maxNeighborNumber | Number of similar items. | Integer |  | 64 |
| userCol | User column name | String | ✓ |  |
| itemCol | Item column name | String | ✓ |  |
| similarityType | similarity type for calculationg neighbor | String |  | "COSINE" |
| rateCol | Rating column name | String |  | null |
| similarityThreshold | threshold | Double |  | 1.0E-4 |

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

model = ItemCfTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRateCol("rating").linkFrom(data);

predictor = ItemCfRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```

### Results
```
	user	item	rating	prediction_result
0	1	1	0.6	0.000000
1	2	2	0.8	0.600000
2	2	3	0.6	0.800000
3	4	1	0.6	0.361237
4	4	2	0.3	0.440631
5	4	3	0.4	0.386137
```
