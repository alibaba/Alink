## Description
Check the size of a vector. if size is not match, then do as handleInvalid

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| size | size of some thing. | Integer | ✓ |  |
| handleInvalidMethod | the handle method of invalid value. include： error, skip | String |  | "ERROR" |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
### Script
``` python
data = np.array(["$8$1:3,2:4,4:7","$8$2:4,4:5"])
df = pd.DataFrame({"vec" : data})
data = dataframeToOperator(df, schemaStr="vec string",op_type="stream")
VectorSizeHintStreamOp().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("Skip").setSize(3).linkFrom(data).print()
StreamOperator.execute()
```
### Result
|vec|vec_hint|
|---|--------|
|$8$1:3,2:4,4:7|$8$1:3.0 2:4.0 4:7.0|
|$8$2:4,4:5|$8$2:4.0 4:5.0|
