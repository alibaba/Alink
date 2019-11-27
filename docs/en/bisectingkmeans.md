## Description
Bisecting k-means is a kind of hierarchical clustering algorithm.
 
 Bisecting k-means algorithm starts from a single cluster that contains all points.
 Iteratively it finds divisible clusters on the bottom level and bisects each of them using
 k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| minDivisibleClusterSize | Minimum divisible cluster size | Integer |  | 1 |
| k | Number of clusters. | Integer |  | 4 |
| distanceType | Distance type for clustering, support EUCLIDEAN and COSINE. | String |  | "EUCLIDEAN" |
| vectorCol | Name of a vector column | String | ✓ |  |
| maxIter | Maximum iterations, The default value is 10 | Integer |  | 10 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "0.1,0.1,0.1"],
    [2, "0.2,0.2,0.2"],
    [3, "9 9 9"],
    [4, "9.1 9.1 9.1"],
    [5, "9.2 9.2 9.2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
kmeans = BisectingKMeans().setVectorCol("vec").setK(2).setPredictionCol("pred")
kmeans.fit(inOp).transform(inOp).collectToDataframe()
```

#### Results
##### Prediction
```
rowId	id	vec	pred
0	0	0 0 0	0
1	1	0.1,0.1,0.1	0
2	2	0.2,0.2,0.2	0
3	3	9 9 9	1
4	4	9.1 9.1 9.1	1
5	5	9.2 9.2 9.2	1
```
