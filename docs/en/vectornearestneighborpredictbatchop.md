## Description
Find the nearest neighbor of query vectors.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| radius | radius | Double |  | null |
| topN | top n | Integer |  | null |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
train = VectorNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec").linkFrom(inOp)
predict = VectorNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3).linkFrom(train, inOp)
predict.print()
```

#### Script输出Result
   id                                                vec
   
0   0  {"ID":"[0,1,2]","METRIC":"[0.0,1.7320508075688...

1   1  {"ID":"[1,2,0]","METRIC":"[0.0,1.7320508075688...

2   2  {"ID":"[2,1,0]","METRIC":"[0.0,1.7320508075688...


