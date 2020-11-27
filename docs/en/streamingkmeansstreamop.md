## Description
Streaming version of Kmeans.
 It supports online learning and inference of kmeans model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| predictionCol | Column name of prediction. | String | ✓ |  |
| halfLife | half life | Integer | ✓ |  |
| predictionDistanceCol | Column name of prediction. | String |  |  |
| predictionClusterCol | Column name of prediction. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| timeInterval | time interval, unit is s. | Long | ✓ |  |

## Script Example

### Code

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)
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
stream_data = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')

kmeans = KMeansTrainBatchOp().setVectorCol("vec").setK(2)
init_model = kmeans.linkFrom(inOp)

streamingkmeans = StreamingKMeansStreamOp(init_model) \
  .setTimeInterval(1) \
  .setHalfLife(1) \
  .setReservedCols(["vec"])

pred = streamingkmeans.linkFrom(stream_data, stream_data)

resetEnv()

```
