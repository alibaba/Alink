## Description
ApproxVectorSimilarityTopNLSHBatchOp used to search the topN nearest neighbor of every record in
 the first dataset from the second dataset. It's an approximate method using LSH.
 
 The two datasets must each contain at least two columns: vector column and id column.
 
 The class supports two distance type: EUCLIDEAND and JACCARD.
 
 The output contains four columns: leftId, rightId, distance, rank.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| distanceType | Distance type for clustering, support EUCLIDEAN and JACCARD. | String |  | "EUCLIDEAN" |
| topN | top n | Integer |  | 5 |
| leftCol | Name of the tensor column from left table | String | ✓ |  |
| rightCol | Name of the tensor column from the right table | String | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
| leftIdCol | Name of the tensor column from left table | String | ✓ |  |
| rightIdCol | Name of the id column from right table | String | ✓ |  |
| projectionWidth | Bucket length, used in bucket random projection LSH. | Double |  | 1.0 |
| numHashTables | The number of hash tables | Integer |  | 1 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| numProjectionsPerTable | The number of hash functions within every hash table | Integer |  | 1 |
| seed | seed | Long |  | 0 |


## Script Example
#### Code
```
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})

source = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
op = (
    ApproxVectorSimilarityTopNLSHBatchOp()
    .setLeftIdCol("id")
    .setRightIdCol("id")
    .setLeftCol("vec")
    .setRightCol("vec")
    .setOutputCol("output"))
op.linkFrom(source, source).collectToDataframe()
```

#### Results

##### Output Data
```
rowID id_right id_left output	rank
0	0	0	0.0	1
1	1	1	0.0	1
2	2	2	0.0	1
```
