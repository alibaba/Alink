## Description
Linear logistic regression predict stream operator. this operator predict data's label with linear model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example
#### Script
```
import numpy as np
import pandas as pd
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 2]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

streamData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='stream')
batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
dataTest = streamData
colnames = ["f0","f1"]
lr = LogisticRegressionTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = LogisticRegressionPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(dataTest).print()
StreamOperator.execute()
```
#### Result
f0 | f1 | label | pred 
---|----|-------|-----
2|1|1|1
3|2|1|1
4|3|2|2
2|4|1|1
2|2|1|1
4|3|2|2
1|2|1|1
5|3|2|2

