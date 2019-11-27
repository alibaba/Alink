## Description
One-hot batch operator maps a serial of columns of category indices to a column of
 sparse binary vectors.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String | ✓ |  |


## Script Example
#### Script
```python
data = np.array([
    ["assisbragasm", 1],
    ["assiseduc", 1],
    ["assist", 1],
    ["assiseduc", 1],
    ["assistebrasil", 1],
    ["assiseduc", 1],
    ["assistebrasil", 1],
    ["assistencialgsamsung", 1]
])

# load data
df = pd.DataFrame({"query": data[:, 0], "weight": data[:, 1]})

inOp = dataframeToOperator(df, schemaStr='query string, weight long', op_type='batch')

# one hot train
one_hot = OneHotTrainBatchOp().setSelectedCols(["query"]).setDropLast(False).setIgnoreNull(False)
model = inOp.link(one_hot)

# batch predict
predictor = OneHotPredictBatchOp().setOutputCol("predicted_r").setReservedCols(["weight"])
print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))

# stream predict
inOp2 = dataframeToOperator(df, schemaStr='query string, weight long', op_type='stream')
predictor = OneHotPredictStreamOp(model).setOutputCol("predicted_r").setReservedCols(["weight"])
predictor.linkFrom(inOp2).print()

StreamOperator.execute()
```
#### Result

```python
  weight predicted_r
0       1    $6$4:1.0
1       1    $6$3:1.0
2       1    $6$2:1.0
3       1    $6$3:1.0
4       1    $6$1:1.0
5       1    $6$3:1.0
6       1    $6$1:1.0
7       1    $6$0:1.0

```






