## Description
One-hot maps a serial of columns of category indices to a column of
 sparse binary vector. It will produce a model of one hot, and then it can transform
 data to binary format using this model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| discreteThresholdsArray | discrete thresholds array | Integer[] |  | |
| discreteThresholds | discrete thresholds array | Integer |  | Integer.MIN_VALUE |
| selectedCols | Names of the columns used for processing | String[] |  |  |

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






