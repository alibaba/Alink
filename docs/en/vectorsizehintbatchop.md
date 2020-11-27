## Description
Check the size of a vector. if size is not match, then do as handleInvalid.
 If error, will throw exception if the vector is null or the vector size doesn't match the given one.
 If optimistic, will accept the vector if it is not null.

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
data = np.array([["$8$1:3,2:4,4:7"],["$8$2:4,4:5"]])
df = pd.DataFrame({"vec" : data[:,0]})
data = dataframeToOperator(df, schemaStr="vec string",op_type="batch")
VectorSizeHintBatchOp().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("SKIP").setSize(8).linkFrom(data).collectToDataframe()

```
### Result
|vec|vec_hint|
|---|--------|
|$8$1:3,2:4,4:7|$8$1:3.0 2:4.0 4:7.0|
|$8$2:4,4:5|$8$2:4.0 4:5.0|
