## Description
Find the approximate nearest neighbor of query string.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| metric | Method to calculate calc or distance. | String |  | "SIMHASH_HAMMING_SIM" |
| radius | radius | Double |  | null |
| topN | top n | Integer |  | null |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| idCol | id colname | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| numBucket | the number of bucket | Integer |  | 10 |
| numHashTables | The number of hash tables | Integer |  | 10 |
| seed | seed | Long |  | 0 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')

pipeline = StringApproxNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("SIMHASH_HAMMING_SIM").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
#### Results
   id   text1                                              text2
   
0   0   abcde  {"ID":"[0,1,2]","METRIC":"[0.953125,0.921875,0...

1   1  aacedw  {"ID":"[0,1,4]","METRIC":"[0.9375,0.90625,0.85...

2   2   cdefa  {"ID":"[0,1,4]","METRIC":"[0.890625,0.859375,0...

3   3   bdefh  {"ID":"[4,2,1]","METRIC":"[0.9375,0.90625,0.89...

4   4   acedm  {"ID":"[1,0,4]","METRIC":"[0.921875,0.921875,0...
