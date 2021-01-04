## Description
Check the size of a vector. if size is not match, then do as handleInvalid.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| size | size of some thing. | Integer | ✓ |  |
| handleInvalidMethod | the handle method of invalid value. include： error, skip | String |  | "ERROR" |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
### Script
``` python
data = np.array([["$8$1:3,2:4,4:7"],["$8$2:4,4:5"]])
df = pd.DataFrame({"vec" : data[:,0]})
data = dataframeToOperator(df, schemaStr="vec string",op_type="batch")
model = VectorSizeHint().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("SKIP").setSize(8)
model.transform(data).collectToDataframe()
```
### Result
|vec|vec_hint|
|---|--------|
|$8$1:3,2:4,4:7|$8$1:3.0 2:4.0 4:7.0|
|$8$2:4,4:5|$8$2:4.0 4:5.0|
