## Description
Transform data type from Columns to Vector.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | Strategy to handle unseen token | String |  | "ERROR" |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| vectorCol | Name of a vector column | String | ✓ |  |
| vectorSize | Size of the vector | Long |  | -1 |
| selectedCols | Names of the columns used for processing | String[] |  | null |

## Script Example
### Code
```python
import numpy as np
import pandas as pd


data = np.array([['1', '{"f1":"1.0","f2":"2.0"}', '$3$1:1.0 2:2.0', '1:1.0,2:2.0', '1.0,2.0', 1.0, 2.0],
['2', '{"f2":"4.0","f4":"8.0"}', '$3$1:4.0 2:8.0', '1:4.0,2:8.0', '4.0,8.0', 4.0, 8.0]])

df = pd.DataFrame({"row":data[:,0], "json":data[:,1], "vec":data[:,2], "kv":data[:,3], "csv":data[:,4], "f0":data[:,5], "f1":data[:,6]})
data = dataframeToOperator(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double",op_type="stream")
    

op = ColumnsToVectorStreamOp()\
    .setSelectedCols(["f0", "f1"])\
    .setReservedCols(["row"]).setVectorCol("vec").setVectorSize(5)\
    .linkFrom(data)
op.print()
StreamOperator.execute()
```

### Results
    
    |row|vec|
    |-|-----|
    |1|$5$1.0 2.0|
    |2|$5$4.0 8.0|
    
