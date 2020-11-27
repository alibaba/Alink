## Description
Map string to index.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "KEEP" |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
### Code
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

df_data = pd.DataFrame({
    "f0": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='batch')
stream_data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='stream')

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)

predictor = StringIndexerPredictStreamOp(model).setSelectedCol("f0").setOutputCol("f0_indexed")
predictor.linkFrom(stream_data).print()
StreamOperator.execute()
resetEnv()

```

### Results

```
['f0', 'f0_indexed']
['football', 2]
['football', 2]
['football', 2]
['basketball', 1]
['basketball', 1]
['tennis', 0]
```
