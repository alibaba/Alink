## Description
Encode one column of strings to bigint type indices.
 The indices are consecutive bigint type that start from 0.
 Non-string columns are first converted to strings and then encoded.
 
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
| modelName | Name of the model | String |  |  |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "keep" |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| stringOrderType | String order type, one of "random", "frequency_asc", "frequency_desc", "alphabet_asc", "alphabet_desc". | String |  | "random" |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |


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

stringindexer = StringIndexer() \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc")

stringindexer.fit(data).transform(data).print()
```

#### Results

```
           f0  f0_indexed
0    football           2
1    football           2
2    football           2
3  basketball           1
4  basketball           1
5      tennis           0
```

