## Description
Find the nearest neighbor of query texts.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| metric | Method to calculate calc or distance. | String |  | "LEVENSHTEIN_SIM" |
| radius | radius | Double |  | null |
| topN | top n | Integer |  | null |
| lambda | punish factor. | Double |  | 0.5 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| idCol | id colname | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| windowSize | window size | Integer |  | 2 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "a b c d e", "a a b c e"],
    [1, "a a c e d w", "a a b b e d"],
    [2, "c d e f a", "b b c e f a"],
    [3, "b d e f h", "d d e a c"],
    [4, "a c e d m", "a e e f b c"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')

pipeline = TextNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").setTopN(3)

pipeline.fit(inOp).transform(inOp).print()
```
#### Results
   id   text1                                              text2
   
0   0   abcde  {"ID":"[0,1,4]","METRIC":"[0.6,0.5,0.199999999...

1   1  aacedw  {"ID":"[1,4,0]","METRIC":"[0.5,0.3333333333333...

2   2   cdefa  {"ID":"[2,3,1]","METRIC":"[0.5,0.5,0.333333333...

3   3   bdefh  {"ID":"[2,3,4]","METRIC":"[0.4,0.4,0.199999999...

4   4   acedm  {"ID":"[2,4,3]","METRIC":"[0.33333333333333337...




