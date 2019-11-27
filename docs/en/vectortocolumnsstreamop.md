## Description
Transform vector to table columns. this class maps vector column to many columns.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCols | Names of the output columns | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |


## Script Example

#### Script

```python
data = np.array([["a", "10.0, 100"],\
    ["b", "-2.5, 9"],\
    ["c", "100.2, 1"],\
    ["d", "-99.9, 100"],\
    ["a", "1.4, 1"],\
    ["b", "-2.2, 9"],\
    ["c", "100.9, 1"]])
df = pd.DataFrame({"col" : data[:,0], "vec" : data[:,1]})
data = dataframeToOperator(df, schemaStr="col string, vec string",op_type="stream")
VectorToColumnsStreamOp().setSelectedCol("vec").setOutputCols(["f0", "f1"]).linkFrom(data).print()
StreamOperator.execute()
```
#### Result

<img src="https://img.alicdn.com/tfs/TB1IMm7oXP7gK0jSZFjXXc5aXXa-232-226.jpg">
