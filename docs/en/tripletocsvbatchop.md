## Description
Transform data type from Triple to Csv.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | Strategy to handle unseen token | String |  | "ERROR" |
| tripleColumnCol | Name of the triple column col | String | ✓ |  |
| tripleValueCol | Name of the triple value column | String | ✓ |  |
| csvCol | Name of the CSV column | String | ✓ |  |
| schemaStr | Formatted schema | String | ✓ |  |
| csvFieldDelimiter | Field delimiter | String |  | "," |
| quoteChar | quote char | Character |  | "\"" |
| tripleRowCol | Name of the triple row column | String |  | null |

## Script Example
### Code
```python
import numpy as np
import pandas as pd


data = np.array([[1,'f1',1.0],[1,'f2',2.0],[2,'f1',4.0],[2,'f2',8.0]])
df = pd.DataFrame({"row":data[:,0], "col":data[:,1], "val":data[:,2]})
data = dataframeToOperator(df, schemaStr="row double, col string, val double",op_type="batch")


op = TripleToCsvBatchOp()\
    .setTripleRowCol("row").setTripleColumnCol("col").setTripleValueCol("val")\
    .setCsvCol("csv").setSchemaStr("f1 string, f2 string")\
    .linkFrom(data)
op.print()
```

### Results
    
|row|csv|
|-|-------|
|1|1.0,2.0|
|2|4.0,8.0|
    
