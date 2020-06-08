## Description
Transform data type from Vector to Columns.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | Strategy to handle unseen token | String |  | "ERROR" |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| schemaStr | Formatted schema | String | ✓ |  |
| vectorCol | Name of a vector column | String | ✓ |  |

## Script Example
### Code
```python
import numpy as np
import pandas as pd


data = np.array([['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

df = pd.DataFrame({"row":data[:,0], "json":data[:,1], "vec":data[:,2], "kv":data[:,3], "csv":data[:,4], "f0":data[:,5], "f1":data[:,6]})
data = dataframeToOperator(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double",op_type="batch")
    

op = VectorToColumnsBatchOp()\
    .setVectorCol("vec")\
    .setReservedCols(["row"]).setSchemaStr("f0 double, f1 double")\
    .linkFrom(data)
op.print()
```

### Results
    
|row|f0|f1|
|-|---|---|
|1|1.0|2.0|
|2|4.0|8.0|
    
