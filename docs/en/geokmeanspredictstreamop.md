## Description
Find  the closest cluster center for every point.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| numThreads | Thread number of operator. | Integer |  | 1 |
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
inOp1 = dataframeToOperator(df, schemaStr='f0 long, f1 long', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='f0 long, f1 long', op_type='stream')
kmeans = KMeans4LongiLatitudeTrainBatchOp().setLongitudeCol("f0").setLatitudeCol("f1").setK(2).linkFrom(inOp1)
kmeans.print()
predict = KMeans4LongiLatitudePredictBatchOp().setPredictionCol("pred").linkFrom(kmeans, inOp1)
predict.print()

predict = KMeans4LongiLatitudePredictStreamOp(kmeans).setPredictionCol("pred").linkFrom(inOp2)
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


##### 预测输出
f0|f1|pred
---|---|----
0|0|1
8|8|0
1|2|1
9|10|0
3|1|1
10|7|0

