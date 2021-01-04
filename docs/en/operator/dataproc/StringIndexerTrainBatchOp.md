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
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
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

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)
model.print()
```

### Results

Model：
```
        token  token_index
0      tennis            0
1  basketball            1
2    football            2
```


