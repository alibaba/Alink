## Description
FpGrowth computes frequent itemsets given a set of transactions.
 The FP-Growth algorithm is described in <a href="http://dx.doi.org/10.1145/335191.335372">
 Han et al., Mining frequent patterns without candidate generation</a>.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| itemsCol | Column name of transaction items | String | ✓ |  |
| minSupportCount | Minimum support count | Integer |  | -1 |
| minSupportPercent | Minimum support percent | Double |  | 0.02 |
| minConfidence | Minimum confidence | Double |  | 0.05 |
| maxPatternLength | Maximum frequent pattern length | Integer |  | 10 |
| maxConsequentLength | Maximum consequent length | Integer |  | 1 |
| minLift | Minimum lift | Double |  | 1.0 |

## Script Example
### Code
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    ["A,B,C,D"],
    ["B,C,E"],
    ["A,B,C,E"],
    ["B,D,E"],
    ["A,B,C,D"],
])

df_data = pd.DataFrame({
    "items": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='items string', op_type='batch')

fpGrowth = FpGrowthBatchOp() \
    .setItemsCol("items") \
    .setMinSupportPercent(0.4) \
    .setMinConfidence(0.6)

fpGrowth.linkFrom(data)

fpGrowth.print()
fpGrowth.getSideOutput(0).print()
resetEnv()

```

### Results

Output

```
    itemset  supportcount  itemcount
0         E             3          1
1       B,E             3          2
2       C,E             2          2
3     B,C,E             2          3
4         D             3          1
5       B,D             3          2
6       C,D             2          2
7     B,C,D             2          3
8       A,D             2          2
9     B,A,D             2          3
10    C,A,D             2          3
11  B,C,A,D             2          4
12        A             3          1
13      B,A             3          2
14      C,A             3          2
15    B,C,A             3          3
16        C             4          1
17      B,C             4          2
18        B             5          1
```

Output

```
        rule  itemcount      lift  support_percent  confidence_percent  transaction_count
0       B=>A          2  1.000000              0.6            0.600000                  3
1       B=>D          2  1.000000              0.6            0.600000                  3
2       B=>C          2  1.000000              0.8            0.800000                  4
3       B=>E          2  1.000000              0.6            0.600000                  3
4       C=>A          2  1.250000              0.6            0.750000                  3
5       C=>B          2  1.000000              0.8            1.000000                  4
6     B,C=>A          3  1.250000              0.6            0.750000                  3
7       A=>C          2  1.250000              0.6            1.000000                  3
8       A=>B          2  1.000000              0.6            1.000000                  3
9       A=>D          2  1.111111              0.4            0.666667                  2
10    B,A=>D          3  1.111111              0.4            0.666667                  2
11    C,A=>B          3  1.000000              0.6            1.000000                  3
12    B,A=>C          3  1.250000              0.6            1.000000                  3
13    C,A=>D          3  1.111111              0.4            0.666667                  2
14  B,C,A=>D          4  1.111111              0.4            0.666667                  2
15      D=>A          2  1.111111              0.4            0.666667                  2
16      D=>B          2  1.000000              0.6            1.000000                  3
17    A,D=>B          3  1.000000              0.4            1.000000                  2
18    B,D=>A          3  1.111111              0.4            0.666667                  2
19    C,D=>B          3  1.000000              0.4            1.000000                  2
20    A,D=>C          3  1.250000              0.4            1.000000                  2
21    C,D=>A          3  1.666667              0.4            1.000000                  2
22  C,A,D=>B          4  1.000000              0.4            1.000000                  2
23  B,A,D=>C          4  1.250000              0.4            1.000000                  2
24  B,C,D=>A          4  1.666667              0.4            1.000000                  2
25      E=>B          2  1.000000              0.6            1.000000                  3
26    C,E=>B          3  1.000000              0.4            1.000000                  2
```



