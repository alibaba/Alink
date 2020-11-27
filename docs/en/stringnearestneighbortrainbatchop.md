## Description
Find the nearest neighbor of query string.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| metric | Method to calculate calc or distance. | String |  | "LEVENSHTEIN_SIM" |
| trainType | id colname | String | ✓ |  |
| lambda | punish factor. | Double |  | 0.5 |
| idCol | id colname | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| windowSize | window size | Integer |  | 2 |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')

train = StringNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").linkFrom(inOp)
predict = StringNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
predict.print()
```
#### Results
   id   text1                                              text2
   
0   0   abcde  {"ID":"[0,1,4]","METRIC":"[0.6,0.5,0.199999999...

1   1  aacedw  {"ID":"[1,4,0]","METRIC":"[0.5,0.3333333333333...

2   2   cdefa  {"ID":"[2,3,1]","METRIC":"[0.5,0.5,0.333333333...

3   3   bdefh  {"ID":"[2,3,4]","METRIC":"[0.4,0.4,0.199999999...

4   4   acedm  {"ID":"[2,4,3]","METRIC":"[0.33333333333333337...



