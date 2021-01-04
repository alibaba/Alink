## Description
Map index to string.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| modelName | Name of the model | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
#### Code
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

stringIndexer = StringIndexer() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc")

indexed = stringIndexer.fit(data).transform(data)

indexToString = IndexToString() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0_indexed") \
    .setOutputCol("f0_indxed_unindexed")

indexToString.transform(indexed).print()
```

#### Results

```
f0|f0_indexed|f0_indxed_unindexed
--|----------|-------------------
football|2|football
football|2|football
football|2|football
basketball|1|basketball
basketball|1|basketball
tennis|0|tennis
```
