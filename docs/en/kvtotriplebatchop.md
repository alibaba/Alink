## Description
Transform data type from Kv to Triple.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | Strategy to handle unseen token | String |  | "ERROR" |
| tripleColumnValueSchemaStr | Schema string of the triple's column and value column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | [] |
| kvCol | Name of the KV column | String | ✓ |  |
| kvColDelimiter | Delimiter used between key-value pairs when data in the input table is in sparse format | String |  | "," |
| kvValDelimiter | Delimiter used between keys and values when data in the input table is in sparse format | String |  | ":" |

## Script Example
### Code
```python
import numpy as np
import pandas as pd


data = np.array([['1', '{"f1":"1.0","f2":"2.0"}', '$3$1:1.0 2:2.0', '1:1.0,2:2.0', '1.0,2.0', 1.0, 2.0],
['2', '{"f2":"4.0","f4":"8.0"}', '$3$1:4.0 2:8.0', '1:4.0,2:8.0', '4.0,8.0', 4.0, 8.0]])

df = pd.DataFrame({"row":data[:,0], "json":data[:,1], "vec":data[:,2], "kv":data[:,3], "csv":data[:,4], "f0":data[:,5], "f1":data[:,6]})
data = dataframeToOperator(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double",op_type="batch")
    

op = KvToTripleBatchOp()\
    .setKvCol("kv")\
    .setReservedCols(["row"]).setTripleColumnValueSchemaStr("col string, val double")\
    .linkFrom(data)
op.print()
```

### Results
    
    |row|col|val|
    |-|-|---|
    |1|1|1.0|
    |1|2|2.0|
    |2|1|4.0|
    |2|2|8.0|
