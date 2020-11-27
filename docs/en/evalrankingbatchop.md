## Description
Evaluation for ranking system.

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
	("{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}"),
	("{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"),
	("{\"object\":\"[1, 2, 3, 4, 5]\"}", "{\"object\":\"[]\"}")
]
df = pd.DataFrame.from_records(data)
inOp = BatchOperator.fromDataframe(df, schemaStr='pred string, label string')
metrics = EvalRankingBatchOp().setPredictionCol('pred').setLabelCol('label').linkFrom(inOp).collectMetrics()
print(metrics.toString())
```

#### Results
```
-------------------------------- Metrics: --------------------------------
microPrecision:0.32
averageReciprocalHitRank:0.5
precision:0.2667
accuracy:0.2667
f1:0.3761
hitRate:0.6667
microRecall:1
microF1:0.4848
subsetAccuracy:0
recall:0.6667
map:0.355
hammingLoss:0.5667
```


