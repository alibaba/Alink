## Description
*
 A one-hot stream operator that maps a serial of columns of category indices to a column of
 sparse binary vectors.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid |  Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String | | "keep" |
| encode | Encode method，"INDEX", "VECTOR", "ASSEMBLED_VECTOR" | String |   |INDEX |
| dropLast | drop last | Boolean |  | true |
| selectedCols | Names of the columns used for processing | String[] |  |  |
| outputCols | Names of the output columns | String[] |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Script
```python
import numpy as np
import pandas as pd
data = np.array([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])
df = pd.DataFrame({"double": data[:, 0], "bool": data[:, 1], "number": data[:, 2], "str": data[:, 3]})

inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')

onehot = OneHotTrainBatchOp().setSelectedCols(["double", "bool", "number", "str"]).setDiscreteThresholds(2)
predictBatch = OneHotPredictBatchOp().setSelectedCols(["double", "bool"]).setEncode("ASSEMBLED_VECTOR").setOutputCols(["pred"]).setDropLast(False)
onehot.linkFrom(inOp1)
predictBatch.linkFrom(onehot, inOp1)
[model,predict] = collectToDataframes(onehot, predictBatch)
print(model)
print(predict)

predictStream = OneHotPredictStreamOp(onehot).setSelectedCols(["double", "bool"]).setEncode("ASSEMBLED_VECTOR").setOutputCols(["vec"])
predictStream.linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```
#### Result

```python
   double   bool  number str            pred
0     1.1   True       2   A  $6$0:1.0 3:1.0
1     1.1  False       2   B  $6$0:1.0 5:1.0
2     1.1   True       1   B  $6$0:1.0 3:1.0
3     2.2   True       1   A  $6$2:1.0 3:1.0

```






