## Description
Calculate the calc between characters in pair.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lambda | punish factor. | Double |  | 0.5 |
| metric | Method to calculate calc or distance. | String |  | "LEVENSHTEIN_SIM" |
| windowSize | window size | Integer |  | 2 |
| numBucket | the number of bucket | Integer |  | 10 |
| numHashTables | The number of hash tables | Integer |  | 10 |
| seed | seed | Long |  | 0 |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
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
inOp1 = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='stream')

op = StringSimilarityPairwiseBatchOp().setSelectedCols(["text1", "text2"]).setMetric("LEVENSHTEIN").setOutputCol("output")
op.linkFrom(inOp1).print()

op = StringSimilarityPairwiseStreamOp().setSelectedCols(["text1", "text2"]).setMetric("COSINE").setOutputCol("output")
op.linkFrom(inOp2).print()
StreamOperator.execute()
```

#### Results
```
   id   text1   text2  output
0   0   abcde   aabce     2.0
1   1  aacedw  aabbed     3.0
2   2   cdefa  bbcefa     3.0
3   3   bdefh   ddeac     3.0
4   4   acedm  aeefbc     4.0
```




