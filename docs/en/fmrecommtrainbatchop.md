## Description
Fm train batch op for recommendation.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| userFeatureCols |  | String[] |  | [] |
| userCategoricalFeatureCols |  | String[] |  | [] |
| itemFeatureCols |  | String[] |  | [] |
| itemCategoricalFeatureCols |  | String[] |  | [] |
| rateCol | Rating column name | String | ✓ |  |
| userCol | User column name | String | ✓ |  |
| itemCol | Item column name | String | ✓ |  |
| withIntercept | Whether has intercept or not, default is true | Boolean |  | true |
| withLinearItem | with linear item. | Boolean |  | true |
| numFactor | number of factor | Integer |  | 10 |
| lambda0 | lambda0 | Double |  | 0.0 |
| lambda1 | lambda1 | Double |  | 0.0 |
| lambda2 | lambda_2 | Double |  | 0.0 |
| numEpochs | num epochs | Integer |  | 10 |
| learnRate | learn rate | Double |  | 0.01 |
| initStdev | init stdev | Double |  | 0.05 |

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

model = FmRecommTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setNumFactor(20)\
    .setRateCol("rating").linkFrom(data);

predictor = FmRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```

### Results
user|	item|	rating|	prediction_result
----|-----|--- |---
1	|1|	0.6|	0.582958
2	|2|	0.8|	0.576914
2	|3|	0.6|	0.508942
4	|1|	0.6|	0.505525
4	|2|	0.3|	0.372908
4	|3|	0.4|	0.347927
