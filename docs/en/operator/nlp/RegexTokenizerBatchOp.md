## Description
If gaps is true, it splits the document with the given pattern. If gaps is false, it extract the tokens matching the
 pattern.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| pattern | If gaps is true, it's used as a delimiter; If gaps is false, it's used as a token | String |  | "\\s+" |
| gaps | If gaps is true, it splits the document with the given pattern. If gaps is false, it extract the tokens matching the pattern | Boolean |  | true |
| minTokenLength | The minimum of token length. | Integer |  | 1 |
| toLowerCase | If true, transform all the words to lower case。 | Boolean |  | true |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
#### Code
```python
data = np.array([
    [0, 'That is an English Book!'],
    [1, 'Do you like math?'],
    [2, 'Have a good day!']
])

df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp1 = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')
op = RegexTokenizerBatchOp().setSelectedCol("text").setGaps(False).setToLowerCase(True).setOutputCol("token").setPattern("\\w+")

op.linkFrom(inOp1).print()

inOp2 = dataframeToOperator(df, schemaStr='id long, text string', op_type='stream')
op = RegexTokenizerStreamOp().setSelectedCol("text").setGaps(False).setToLowerCase(True).setOutputCol("token").setPattern("\\w+")
op.linkFrom(inOp2).print()

StreamOperator.execute()
```

#### Results

```
id	text	token
0	0	That is an English Book!	that is an english book
1	2	Have a good day!	have a good day
2	1	Do you like math?	do you like math

```
