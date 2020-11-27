## Description
key to value.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| mapKeyCol | the name of the key column in map data table. | String | ✓ |  |
| mapValueCol | the name of the value column in map data table. | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
### Script
```python
import numpy as np
import pandas as pd
data = np.array([
                ["0L", "1L", 0.6],
                ["2L", "2L", 0.8],
                ["2L", "4L", 0.6],
                ["3L", "1L", 0.6],
                ["3L", "2L", 0.3],
                ["3L", "4L", 0.4]
        ])
df = pd.DataFrame({"uid": data[:, 0], "iid": data[:, 1], "label": data[:, 2]})

source = dataframeToOperator(df, schemaStr='uid string, iid string, label double', op_type='batch')

mapData = np.array([
                ["0L", 0],
                ["1L", 1],
                ["2L", 2],
                ["3L", 3],
                ["4L", 4]
        ])
mapDf = pd.DataFrame({"keycol": mapData[:, 0], "valcol": mapData[:, 1]})

mapSource = dataframeToOperator(mapDf, schemaStr='keycol string, valcol int', op_type='batch')

keyToValueOp = KeyToValueBatchOp()\
    .setSelectedCol("uid")\
    .setMapKeyCol("keycol")\
    .setMapValueCol("valcol")

keyToValueOp.linkFrom(mapSource, source)
keyToValueOp.print()
```
### Result
uid|	iid|	label
---| --- | --- |
	0|	1L|	0.6|
	2|	2L|	0.8|
	2|	4L|	0.6
	3|	1L|	0.6
	3|	2L|	0.3
	3|	4L|	0.4


