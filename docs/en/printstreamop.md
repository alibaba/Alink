## Description
Print Stream op to screen.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| refreshInterval | refresh interval | Integer |  | -1 |
| maxLimit | max limit | Integer |  | 100 |

## Script Example

### Code

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
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='stream')

inOp.link(PrintStreamOp())
StreamOperator.execute()

```

### Results

```
id  text1  text2
0  abcde  aabce
1  aacedw  aabbed
2  cdefa  bbcefa
3  bdefh  ddeac
4  acedm  aeefbc
```
