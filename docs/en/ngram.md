## Description
Transform a document into a new document composed of all its ngrams. The document is splitted into
 an array of words by a word delimiter(default space). Through sliding the word array, we get all ngrams
 and each ngram is connected by a "_" character. All the ngrams are joined together with space in the
 new document.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| n | NGram length | Integer |  | 2 |
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
import numpy as np
import pandas as pd

data = np.array([
    [0, 'That is an English Book!'],
    [1, 'Do you like math?'],
    [2, 'Have a good day!']
])

df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp1 = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')

op = NGram().setSelectedCol("text")
op.transform(inOp1).print()
```

#### Results
```
	id	text
0	2	Have_a a_good good_day!
1	1	Do_you you_like like_math?
2	0	That_is is_an an_English English_Book!

```
