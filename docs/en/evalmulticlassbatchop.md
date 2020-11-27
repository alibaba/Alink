## Description
Multi classification evaluation.
 
 Calculate the evaluation metrics for multi classification.
 
 You can either give label column and predResult column or give label column and predDetail column. Once predDetail
 column is given, the predResult column is ignored.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| predictionCol | Column name of prediction. | String |  |  |
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

#### Results
```
Prefix0 accuracy: 0.6
Prefix1 recall: 1.0
Macro Precision: 0.3
Micro Recall: 0.6
Weighted Sensitivity: 0.6
```



