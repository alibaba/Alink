## Description
Bisecting k-means is a kind of hierarchical clustering algorithm.
 
 Bisecting k-means algorithm starts from a single cluster that contains all points. Iteratively it finds divisible
 clusters on the bottom level and bisects each of them using k-means, until there are `k` leaf clusters in total or no
 leaf clusters are divisible.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| minDivisibleClusterSize | Minimum divisible cluster size | Integer |  | 1 |
| k | Number of clusters. | Integer |  | 4 |
| distanceType | Distance type for clustering | String |  | "EUCLIDEAN" |
| vectorCol | Name of a vector column | String | ✓ |  |
| maxIter | Maximum iterations, The default value is 10 | Integer |  | 10 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |

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
inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
kmeans = BisectingKMeansTrainBatchOp().setVectorCol("vec").setK(2)
predictBatch = BisectingKMeansPredictBatchOp().setPredictionCol("pred")
kmeans.linkFrom(inOp1)
predictBatch.linkFrom(kmeans, inOp1)
[model,predict] = collectToDataframes(kmeans, predictBatch)
print(model)
print(predict)

predictStream = BisectingKMeansPredictStreamOp(kmeans).setPredictionCol("pred")
predictStream.linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### Results
##### Model
```
rowId   model_id                                         model_info
0         0  {"vectorCol":"\"vec\"","distanceType":"\"EUCLI...
1   1048576  {"clusterId":1,"size":6,"center":{"data":[4.6,...
2   2097152  {"clusterId":2,"size":3,"center":{"data":[0.1,...
3   3145728  {"clusterId":3,"size":3,"center":{"data":[9.1,...
```

##### Prediction
```
rowId   id          vec  pred
0   0        0 0 0     0
1   1  0.1,0.1,0.1     0
2   2  0.2,0.2,0.2     0
3   3        9 9 9     1
4   4  9.1 9.1 9.1     1
5   5  9.2 9.2 9.2     1
```





