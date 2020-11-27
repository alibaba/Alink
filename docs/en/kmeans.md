## Description
k-mean clustering is a method of vector quantization, originally from signal processing, that is popular for cluster
 analysis in data mining. k-mean clustering aims to partition n observations into k clusters in which each
 observation belongs to the cluster with the nearest mean, serving as a prototype of the cluster.
 
 (https://en.wikipedia.org/wiki/K-means_clustering)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| distanceType | Distance type for clustering | String |  | "EUCLIDEAN" |
| vectorCol | Name of a vector column | String | ✓ |  |
| maxIter | Maximum iterations, the default value is 20 | Integer |  | 50 |
| initMode | Methods to get initial centers, support K_MEANS_PARALLEL and RANDOM! | String |  | "RANDOM" |
| initSteps | When initMode is K_MEANS_PARALLEL, it defines the steps of iteration. The default value is 2. | Integer |  | 2 |
| numThreads | Thread number of operator. | Integer |  | 1 |
| predictionDistanceCol | Column name of prediction. | String |  |  |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| k | Number of clusters. | Integer |  | 2 |
| epsilon | When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges! | Double |  | 1.0E-4 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

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
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
kmeans = KMeans().setVectorCol("vec").setK(2).setPredictionCol("pred")
kmeans.fit(inOp).transform(inOp).collectToDataframe()
```

#### Results
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
