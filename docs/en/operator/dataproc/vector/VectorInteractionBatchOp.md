## Description
VectorInteraction is a Transformer which takes vector or double-valued columns, and generates a single vector column
 that contains the product of all combinations of one value from each input column.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| outputCol | Name of the output column | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example

#### Script
``` python
data = np.array([["$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"],\
    ["$8$0:3,5:5", "$8$1:2,2:4,4:7"],\
    ["$8$2:4,4:5", "$8$1:3,2:3,4:7"]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec1 string, vec2 string",op_type="batch")
vecInter = VectorInteractionBatchOp().setSelectedCols(["vec1","vec2"]).setOutputCol("vec_product")
vecInter.linkFrom(data).collectToDataframe()
```
### Result


| vec1           | vec2           | vec_product                              |
| -------------- | -------------- | ---------------------------------------- |
| $8$1:3,2:4,4:7 | $8$1:3,2:4,4:7 | $64$9:9.0 10:12.0 12:21.0 17:12.0 18:16.0 20:28.0 33:21.0 34:28.0 36:49.0 |
| $8$0:3,5:5     | $8$1:2,2:4,4:7 | $64$8:6.0 13:10.0 16:12.0 21:20.0 32:21.0 37:35.0 |
| $8$2:4,4:5     | $8$1:3,2:3,4:7 | $64$10:12.0 12:15.0 18:12.0 20:15.0 34:28.0 36:35.0 |
