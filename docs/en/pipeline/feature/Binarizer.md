## Description
Binarize a continuous variable using a threshold.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| threshold | Binarization threshold, when number is greater than or equal to threshold, it will be set 1.0, else 0.0. | Double |  | 0.0 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
#### Code
```python
# -*- coding=UTF-8 -*-
import numpy as np
import pandas as pd
data = np.array([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])
df = pd.DataFrame({"double": data[:, 0], "bool": data[:, 1], "number": data[:, 2], "str": data[:, 3]})

inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
binarizer = Binarizer().setSelectedCol("double").setThreshold(2.0)
binarizer.transform(inOp).print()
```
#### Results

##### Output Data
```
rowID   double   bool  number str
0     0.0   True       2   A
1     0.0  False       2   B
2     0.0   True       1   B
3     1.0   True       1   A
```
