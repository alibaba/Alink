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
| k | Number of clusters. | Integer |  | 2 |
| epsilon | When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges! | Double |  | 1.0E-4 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |

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
inOp1 = dataframeToOperator(df, schemaStr='f0 long, f1 long', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='f0 long, f1 long', op_type='stream')
kmeans = GeoKMeansTrainBatchOp().setLongitudeCol("f0").setLatitudeCol("f1").setK(2).linkFrom(inOp1)
kmeans.print()
predict = GeoKMeansPredictBatchOp().setPredictionCol("pred").linkFrom(kmeans, inOp1)
predict.print()

predict = GeoKMeansPredictStreamOp(kmeans).setPredictionCol("pred").linkFrom(inOp2)
predict.print()
StreamOperator.execute()
```
#### Results
##### Model
model_id|model_info
--------|----------
0|{"vectorCol":null,"latitudeCol":"\"f1\"","longitudeCol":"\"f0\"","distanceType":"\"HAVERSINE\"","k":"2","vectorSize":"2"}
1048576|{"clusterId":0,"weight":3.0,"center":"[8.333333333333332, 9.0]","vec":null}
2097152|{"clusterId":1,"weight":3.0,"center":"[1.0, 1.3333333333333333]","vec":null}
2097152|{"center":"{\"data\":[1.0,1.3333333333333333]}","clusterId":1,"weight":3.0}


##### 预测输出
f0|f1|pred
---|---|----
0|0|1
8|8|0
1|2|1
9|10|0
3|1|1
10|7|0



