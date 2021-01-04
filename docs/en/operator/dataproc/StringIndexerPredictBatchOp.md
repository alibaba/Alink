## Description
Map string to index.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "KEEP" |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
### Code
```python
data = np.array([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

df_data = pd.DataFrame({
    "f0": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="batch")

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

predictor = StringIndexerPredictBatchOp().setSelectedCol("f0").setOutputCol("f0_indexed")

model = stringindexer.linkFrom(data)
predictor.linkFrom(model, data).print()
```

### Results

```
           f0  f0_indexed
0    football           2
1    football           2
2    football           2
3  basketball           1
4  basketball           1
5      tennis           0
```
