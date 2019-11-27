## Description
Map a continuous variable into several buckets.

 It supports a single column input or multiple columns input. If input is a single column, selectedColName,
 outputColName and splits should be set. If input are multiple columns, selectedColNames, outputColnames
 and splitsArray should be set, and the lengths of them should be equal. In the case of multiple columns,
 each column used the corresponding splits.

 Split array must be strictly increasing and have at least three points. It's a string input with split points
 segments with delimiter ",".

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| handleInvalid | parameter for how to handle invalid data (NULL values) | String |  | "error" |
| selectedCols | Names of the columns used for processing | String[] |  |  |
| splitsArray | Split points array, each of them is used for the corresponding selected column. | String[] |  |  |
| outputCols | Names of the output columns | String[] |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example
#### Code
```
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
bucketizer = Bucketizer().setSelectedCols(["double"]).setSplitsArray(["-Infinity:2:Infinity"])
bucketizer.transform(inOp).print()
```
#### Results

##### Output Data
```
rowID   double   bool  number str
0       0   True       2   A
1       0  False       2   B
2       0   True       1   B
3       1   True       1   A
```
