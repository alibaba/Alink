## Description
Extract all words and their counts of occurrences from documents.
 Words are recognized by using the delimiter.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| wordDelimiter | Delimiter of words | String |  | " " |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
records = [
    ("doc0", "中国 的 文化"),
    ("doc1", "只要 功夫 深"),
    ("doc2", "北京 的 拆迁"),
    ("doc3", "人名 的 名义")
]
df = pd.DataFrame.from_records(records)
source = BatchOperator.fromDataframe(df, "id string, content string")
wordCountBatchOp = WordCountBatchOp()\
    .setSelectedCol("content")\
    .setWordDelimiter(" ")\
    .linkFrom(source)
wordCountBatchOp.print()
```

#### Results
word|cnt
----|---
人名|1
北京|1
只要|1
名义|1
文化|1
中国|1
功夫|1
拆迁|1
深|1
的|3
