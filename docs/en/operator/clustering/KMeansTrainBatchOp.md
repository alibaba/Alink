## Description
k-mean clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 analysis in data mining. k-mean clustering aims to partition n observations into k clusters in which each
 observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 
 (https://en.wikipedia.org/wiki/K-means_clustering)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| distanceType | Distance type for clustering | String |  | "EUCLIDEAN" |
| vectorCol | Name of a vector column | String | ✓ |  |
| maxIter | Maximum iterations, the default value is 20 | Integer |  | 50 |
| initMode | Methods to get initial centers, support K_MEANS_PARALLEL and RANDOM! | String |  | "RANDOM" |
| initSteps | When initMode is K_MEANS_PARALLEL, it defines the steps of iteration. The default value is 2. | Integer |  | 2 |
| k | Number of clusters. | Integer |  | 2 |
| epsilon | When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges! | Double |  | 1.0E-4 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |

## Script Example
#### Code
```
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
kmeans = KMeansTrainBatchOp().setVectorCol("vec").setK(2)
predictBatch = KMeansPredictBatchOp().setPredictionCol("pred")
kmeans.linkFrom(inOp1)
predictBatch.linkFrom(kmeans, inOp1)
[model,predict] = collectToDataframes(kmeans, predictBatch)
print(model)
print(predict)

predictStream = KMeansPredictStreamOp(kmeans).setPredictionCol("pred")
predictStream.linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### Results
##### Model
```
model_id                                         model_info
0         0  {"vectorCol":"\"vec\"","latitudeCol":null,"lon...
1   1048576  {"clusterId":0,"weight":6.0,"vec":{"data":[9.0...
2   2097152  {"clusterId":1,"weight":6.0,"vec":{"data":[0.1...
```

##### Prediction
```
rowID   id          vec  pred
0   0        0 0 0     1
1   1  0.1,0.1,0.1     1
2   2  0.2,0.2,0.2     1
3   3        9 9 9     0
4   4  9.1 9.1 9.1     0
5   5  9.2 9.2 9.2     0
```





