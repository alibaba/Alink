## Description
Transform data type from Triple to Vector.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | Strategy to handle unseen token | String |  | "ERROR" |
| tripleColumnCol | Name of the triple column col | String | ✓ |  |
| tripleValueCol | Name of the triple value column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| vectorCol | Name of a vector column | String | ✓ |  |
| vectorSize | Size of the vector | Long |  | -1 |
| tripleRowCol | Name of the triple row column | String |  |  |

## Script Example
### Code
```python
import numpy as np
import pandas as pd


data = np.array([[1,'1',1.0],[1,'2',2.0]])
df = pd.DataFrame({"row":data[:,0], "col":data[:,1], "val":data[:,2]})
data = dataframeToOperator(df, schemaStr="row double, col string, val double",op_type="batch")


op = TripleToVectorBatchOp()\
    .setTripleRowCol("row").setTripleColCol("col").setTripleValCol("val")\
    .setReservedCols(["row"]).setVectorCol("vec").setVectorSize(5)\
    .linkFrom(data)
op.print()
```

### Results
    
    |row|vec|
    |-|-----|
    |1|$5$1.0 2.0|
    
