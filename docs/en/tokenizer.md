## Description
Transform all words into lower case, and remove extra space.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


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

op = Tokenizer().setSelectedCol("text")
op.transform(inOp1).print()
```

#### Results
```
	id	text
0	1	do you like math?
1	0	that is an english book!
2	2	have a good day!

```




