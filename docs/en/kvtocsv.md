## Description
Transform data type from Kv to Csv.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | Strategy to handle unseen token | String |  | "ERROR" |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| csvCol | Name of the CSV column | String | ✓ |  |
| schemaStr | Formatted schema | String | ✓ |  |
| csvFieldDelimiter | Field delimiter | String |  | "," |
| quoteChar | quote char | Character |  | "\"" |
| kvCol | Name of the KV column | String | ✓ |  |
| kvColDelimiter | Delimiter used between key-value pairs when data in the input table is in sparse format | String |  | "," |
| kvValDelimiter | Delimiter used between keys and values when data in the input table is in sparse format | String |  | ":" |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
### Code
```python
import numpy as np
import pandas as pd


data = np.array([['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

df = pd.DataFrame({"row":data[:,0], "json":data[:,1], "vec":data[:,2], "kv":data[:,3], "csv":data[:,4], "f0":data[:,5], "f1":data[:,6]})
data = dataframeToOperator(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double",op_type="batch")
    

op = KvToCsv()\
    .setKvCol("kv")\
    .setReservedCols(["row"]).setCsvCol("csv").setSchemaStr("f0 double, f1 double")\
    .transform(data)
op.print()
```

### Results
    
|row|csv|
|-|-------|
|1|1.0,2.0|
|2|4.0,8.0|
    
