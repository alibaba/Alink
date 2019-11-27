## Description
Calculate the evaluation metrics for binary classifiction.

 You can either give label column and predResult column or give label column and predDetail column.
 Once predDetail column is given, the predResult column is ignored.

 PositiveValue is optional, if given, it will be placed at the first position in the output label Array.
 If not given, the labels are sorted in descending order.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| positiveLabelValueString | positive label value with string format. | String |  | null |


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

metrics = EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp).collectMetrics()
print("AUC:", metrics.getAuc())
print("KS:", metrics.getKs())
print("PRC:", metrics.getPrc())
print("Accuracy:", metrics.getAccuracy())
print("Macro Precision:", metrics.getMacroPrecision())
print("Micro Recall:", metrics.getMicroRecall())
print("Weighted Sensitivity:", metrics.getWeightedSensitivity())
```

#### Results
```
AUC: 0.8333333333333333
KS: 0.6666666666666666
PRC: 0.9027777777777777
Accuracy: 0.6
Macro Precision: 0.3
Micro Recall: 0.6
Weighted Sensitivity: 0.6
```


