## Description
Fm regression predict stream operator. this operator predict data's label with fm model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
#### Script
```python
import numpy as np
import pandas as pd
data = np.array([
    ["1:1.1 3:2.0", 1.0],
    ["2:2.1 10:3.1", 1.0],
    ["1:1.2 5:3.2", 0.0],
    ["3:1.2 7:4.2", 0.0]])
df = pd.DataFrame({"kv": data[:, 0], 
                   "label": data[:, 1]})

input = dataframeToOperator(df, schemaStr='kv string, label double', op_type='batch')
test = dataframeToOperator(df, schemaStr='kv string, label double', op_type='stream')
# load data
fm = FmRegressorTrainBatchOp().setVectorCol("kv").setLabelCol("label")
model = input.link(fm)

predictor = FmRegressorPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(test).print()
StreamOperator.execute()
```
#### Result
kv	| label	| pred
---|----|-------
1:1.1 3:2.0|1.0|0.473600
2:2.1 10:3.1|1.0|0.755115
1:1.2 5:3.2|0.0|0.005875
3:1.2 7:4.2|0.0|0.004641





