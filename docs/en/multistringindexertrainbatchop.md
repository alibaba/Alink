## Description
Encode several columns of strings to bigint type indices. The indices are consecutive bigint type
 that start from 0. Non-string columns are first converted to strings and then encoded. Each columns
 are encoded separately.
 
 Several string order type is supported, including:
 <ol>
 <li>random</li>
 <li>frequency_asc</li>
 <li>frequency_desc</li>
 <li>alphabet_asc</li>
 <li>alphabet_desc</li>
 </ol>

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| stringOrderType | String order type, one of "random", "frequency_asc", "frequency_desc", "alphabet_asc", "alphabet_desc". | String |  | "RANDOM" |

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

stringindexer = MultiStringIndexerTrainBatchOp() \
    .setSelectedCols(["f0"]) \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)
model.print()
```

### Results

Model：
```
   column_index                                              token  token_index
0            -1  {"selectedCols":"[\"f0\"]","selectedColTypes":...          NaN
1             0                                             tennis          0.0
2             0                                         basketball          1.0
3             0                                           football          2.0
```
