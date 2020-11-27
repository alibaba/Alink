## Description
This version of kmeans support haversine distance, which is used to calculate the great-circle distance.
 
 (https://en.wikipedia.org/wiki/Haversine_formula)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| maxIter | Maximum iterations, the default value is 20 | Integer |  | 50 |
| initMode | Methods to get initial centers, support K_MEANS_PARALLEL and RANDOM! | String |  | "RANDOM" |
| initSteps | When initMode is K_MEANS_PARALLEL, it defines the steps of iteration. The default value is 2. | Integer |  | 2 |
| latitudeCol | latitude col name | String | ✓ |  |
| longitudeCol | longitude col name | String | ✓ |  |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| k | Number of clusters. | Integer |  | 2 |
| epsilon | When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges! | Double |  | 1.0E-4 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |
| numThreads | Thread number of operator. | Integer |  | 1 |
| predictionDistanceCol | Column name of prediction. | String |  |  |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, 0],
	[8, 8],
	[1, 2],
	[9, 10],
	[3, 1],
	[10, 7]
])
df = pd.DataFrame({"f0": data[:, 0], "f1": data[:, 1]})
inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 long, f1 long')
kmeans = KMeans4LongiLatitude().setLongitudeCol("f0").setLatitudeCol("f1").setK(2).setPredictionCol("pred")
kmeans.fit(inOp1).transform(inOp1).print()
```
#### Results
f0|f1|pred
---|---|----
0|0|1
8|8|0
1|2|1
9|10|0
3|1|1
10|7|0



