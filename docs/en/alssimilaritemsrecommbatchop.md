## Description
Recommend similar items for the given item.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| itemCol | Item column name | String | ✓ |  |
| k | Number of the recommended top objects. | Integer |  | 10 |
| recommCol | Column name of recommend result. | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

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

als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)

model = als.linkFrom(data)
predictor = AlsSimilarItemsRecommBatchOp() \
    .setItemCol("item").setRecommCol("rec").setK(1).setReservedCols(["item"])

predictor.linkFrom(model, data).print();

```

### Results

item| rec
----|-------
1	|{"object":"[3]","score":"[0.8821980357170105]"}
2	|{"object":"[3]","score":"[0.9917739629745483]"}
3	|{"object":"[2]","score":"[0.9917739629745483]"}
1	|{"object":"[3]","score":"[0.8821980357170105]"}
2	|{"object":"[3]","score":"[0.9917739629745483]"}
3	|{"object":"[2]","score":"[0.9917739629745483]"}
