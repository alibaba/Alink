## Description
Recommend items for user with itemCF model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| userCol | User column name | String | ✓ |  |
| k | Number of the recommended top objects. | Integer |  | 10 |
| excludeKnown | Flag of excluding the known objects in recommended top objects. | Boolean |  | false |
| recommCol | Column name of recommend result. | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

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

predictor = ItemCfItemsPerUserRecommender()\
    .setUserCol("user")\
    .setReservedCols(["user"])\
    .setK(1)\
    .setRecommCol("prediction_result")\
    .setModelData(model)

predictor.transform(data).print()
```

### Results
```
	user	prediction_result
0	1	{"object":"[3]","score":"[0.23533936216582085]"}
1	2	{"object":"[3]","score":"[0.38953648389671724]"}
2	2	{"object":"[3]","score":"[0.38953648389671724]"}
3	4	{"object":"[2]","score":"[0.17950184794838112]"}
4	4	{"object":"[2]","score":"[0.17950184794838112]"}
5	4	{"object":"[2]","score":"[0.17950184794838112]"}
```
