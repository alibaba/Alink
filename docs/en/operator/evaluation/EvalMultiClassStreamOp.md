## Description
Calculate the evaluation data within time windows for multi classifiction.
 You can either give label column and predResult column or give label column and predDetail column.
 Once predDetail column is given, the predResult column is ignored.
 The labels are sorted in descending order in the output label array and confusion matrix..

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| predictionCol | Column name of prediction. | String |  |  |
| timeInterval | Time interval of streaming windows, unit s. | Double |  | 3.0 |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |

## Script Example
#### Code
```
import numpy as np
import pandas as pd
data = np.array([
    ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
	["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
	["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
	["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
	["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]])
	
df = pd.DataFrame({"label": data[:, 0], "detailInput": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')

metrics = EvalMultiClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp).collectMetrics()
print("Prefix0 accuracy:", metrics.getAccuracy("prefix0"))
print("Prefix1 recall:", metrics.getRecall("prefix1"))
print("Macro Precision:", metrics.getMacroPrecision())
print("Micro Recall:", metrics.getMicroRecall())
print("Weighted Sensitivity:", metrics.getWeightedSensitivity())


inOp = StreamOperator.fromDataframe(df, schemaStr='label string, detailInput string')
EvalMultiClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(1).linkFrom(inOp).print()
StreamOperator.execute()
```



