## Description
It is summary of table, support count, mean, variance, min, max, sum.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] |  | null |


## Script Example

#### Script

```python
data = np.array([
    ["a", 1, 1,2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2,2.0, False],
    ["c", 0, 0, 0.0, False]
])
df = pd.DataFrame({"f_string": data[:, 0], "f_long": data[:, 1], "f_int": data[:, 2], "f_double": data[:, 3], "f_boolean": data[:, 4]})
source = dataframeToOperator(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean', op_type='batch')

summarizer = SummarizerBatchOp()\
    .setSelectedCols(["f_long", "f_int", "f_double"])

summary = summarizer.linkFrom(source).collectSummary()

print(summary.sum('f_double'))
```
#### Result

```
1.0
```
