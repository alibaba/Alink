## Description
this pipeline rating user item pair with als model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| userCol | User column name | String | ✓ |  |
| itemCol | Item column name | String | ✓ |  |
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

useLocalEnv(1, config=None)

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

alsRate = AlsRateRecommender().setUserCol("user").setItemCol("item").setRecommCol("rating") \
        .setRecommCol("predicted_rating").setModelData(model)
    
alsRate.transform(data).print()
```

### Results

```
   user  item  rating  predicted_rating
0     1     1     0.6          0.579622
1     2     2     0.8          0.766851
2     2     3     0.6          0.581079
3     4     1     0.6          0.574481
4     4     2     0.3          0.298500
5     4     3     0.4          0.382157
```




