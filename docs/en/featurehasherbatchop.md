## Description
Projects a number of categorical or numerical features into a feature vector of a specified dimension.

 (https://en.wikipedia.org/wiki/Feature_hashing)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| numFeatures | The number of features. It will be the length of the output vector. | Integer |  | 262144 |
| categoricalCols | Names of the categorical columns used for training in the input table | String[] |  |  |

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

inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')

hasher = FeatureHasherBatchOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
hasher.linkFrom(inOp1).print()

hasher = FeatureHasherStreamOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
hasher.linkFrom(inOp2).print()

StreamOperator.execute()
```

#### Results

##### Output Data
```
   double   bool  number str                             output
0     1.1   True       2   A  $200$13:2.0 38:1.1 45:1.0 195:1.0
1     1.1  False       2   B   $200$13:2.0 30:1.0 38:1.1 76:1.0
2     1.1   True       1   B  $200$13:1.0 38:1.1 76:1.0 195:1.0
3     2.2   True       1   A  $200$13:1.0 38:2.2 45:1.0 195:1.0
```
