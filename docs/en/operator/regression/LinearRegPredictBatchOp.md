## Description
Linear regression predict batch operator.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
### Code
``` python
import numpy as np
import pandas as pd
from pyalink.alink import *
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
colnames = ["f0","f1"]
lr = LinearRegTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = LinearRegPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```

### Result
f0 | f1 | label | pred
---|----|-------|-----
   2 |  1   |   1  | 1.000014
   3 |  2   |   1  | 1.538474
   4 |  3   |   2  | 2.076934
   2 |  4   |   1  | 1.138446
   2 |  2   |   1  | 1.046158
   4 |  3   |   2  | 2.076934
   1 |  2   |   1  | 0.553842
   5 |  3   |   3  | 2.569250





