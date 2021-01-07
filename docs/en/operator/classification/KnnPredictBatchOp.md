## Description
KNN batch predictor.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| k | k | Integer |  | 10 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```
from pyalink.alink import *
import numpy as np
import pandas as pd

useLocalEnv(2)

data = np.array([
    [1, 0, 0],
    [2, 8, 8],
    [1, 1, 2],
    [2, 9, 10],
    [1, 3, 1],
    [2, 10, 7]
])
df = pd.DataFrame({"label": data[:, 0], "f0": data[:, 1], "f1": data[:, 2]})

dataSourceOp = dataframeToOperator(df, schemaStr="label int, f0 int, f1 int", op_type='batch')
trainOp = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
predictOp = KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp)
predictOp.print()
```

#### Results
```
   label  f0  f1  pred
0      1   0   0     1
1      2   8   8     2
2      1   1   2     1
3      2   9  10     2
4      1   3   1     1
5      2  10   7     2
```
