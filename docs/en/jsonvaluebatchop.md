## Description
Extract json value from json string.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| jsonPath |  json path | String[] | ✓ |  |
| skipFailed |  skip Failed | boolean |  | false |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCols | Names of the output columns | String[] | ✓ |  |


## Script Example
#### Script
```python

import numpy as np
import pandas as pd
data = np.array([
    ["{a:boy,b:{b1:1,b2:2}}"],
    ["{a:girl,b:{b1:1,b2:2}}"]])
df = pd.DataFrame({"str": data[:, 0]})

batchData = dataframeToOperator(df, schemaStr='str string', op_type='batch')

JsonValueBatchOp().setJsonPath(["$.a","$.b.b1"]).setSelectedCol("str").setOutputCols(["f0","f1"]).linkFrom(batchData).print()
```

#### Result

str | f0 | f1
----|----|---
{a:boy,b:{b1:1,b2:2}}|boy|1
{a:girl,b:{b1:1,b2:2}}|girl|1





