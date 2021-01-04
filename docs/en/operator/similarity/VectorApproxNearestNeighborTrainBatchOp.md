## Description
Find the approximate nearest neighbor of query vectors.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| metric | Distance type for clustering | String |  | "EUCLIDEAN" |
| solver | Method to calc approx topN. | String |  | "KDTREE" |
| trainType | id colname | String | ✓ |  |
| idCol | id colname | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| numHashTables | The number of hash tables | Integer |  | 1 |
| numProjectionsPerTable | The number of hash functions within every hash table | Integer |  | 1 |
| seed | seed | Long |  | 0 |
| projectionWidth | Bucket length, used in bucket random projection LSH. | Double |  | 1.0 |

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
train = VectorApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec").linkFrom(inOp)
predict = VectorApproxNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3).linkFrom(train, inOp)
predict.print()
```

#### Script输出Result
   id                                                vec
   
0   0  {"ID":"[0,1,2]","METRIC":"[0.0,1.7320508075688...

1   1  {"ID":"[1,2,0]","METRIC":"[0.0,1.7320508075688...

2   2  {"ID":"[2,1,0]","METRIC":"[0.0,1.7320508075688...


