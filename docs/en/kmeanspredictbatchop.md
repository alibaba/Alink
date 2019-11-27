## Description
Find  the closest cluster center for every point.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| predictionDistanceCol | Column name of prediction. | String |  |  |
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




