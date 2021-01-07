## Description
Evaluation for multi-label classification task.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| predictionRankingInfo | the label of ranking in prediction col | String |  | "object" |
| labelRankingInfo | the label of ranking in label col | String |  | "object" |
| predictionCol | Column name of prediction. | String | ✓ |  |

## Script Example
#### Code

```
import numpy as np
import pandas as pd

data = [
    ("{\"object\":\"[0.0, 1.0]\"}", "{\"object\":\"[0.0, 2.0]\"}"),
    ("{\"object\":\"[0.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
    ("{\"object\":\"[]\"}", "{\"object\":\"[0.0]\"}"),
    ("{\"object\":\"[2.0]\"}", "{\"object\":\"[2.0]\"}"),
    ("{\"object\":\"[2.0, 0.0]\"}", "{\"object\":\"[2.0, 0.0]\"}"),
    ("{\"object\":\"[0.0, 1.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
    ("{\"object\":\"[1.0]\"}", "{\"object\":\"[1.0, 2.0]\"}")
]

df = pd.DataFrame.from_records(data)
source = BatchOperator.fromDataframe(df, "pred string, label string")

evalMultiLabelBatchOp: EvalMultiLabelBatchOp = EvalMultiLabelBatchOp().setLabelCol("label").setPredictionCol("pred").linkFrom(source)
metrics = evalMultiLabelBatchOp.collectMetrics()
print(metrics)
```

#### Results
```
-------------------------------- Metrics: --------------------------------
microPrecision:0.7273
microF1:0.6957
subsetAccuracy:0.2857
precision:0.6667
recall:0.6429
accuracy:0.5476
f1:0.6381
microRecall:0.6667
hammingLoss:0.3333
```
