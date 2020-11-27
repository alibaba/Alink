## Description
Polynomial expansion is the process of expanding your features into a polynomial space, which is formulated by an
 n-degree combination of original dimensions. Take a 2-variable feature vector as an example: (x, y), if we want to
 expand it with degree 2, then we get (x, x * x, y, x * y, y * y).

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| degree | degree of polynomial expand. | Integer |  | 2 |
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
data = np.array([["$8$1:3,2:4,4:7"],
    ["$8$2:4,4:5"]])
df = pd.DataFrame({"vec" : data[:,0]})
data = dataframeToOperator(df, schemaStr="vec string",op_type="batch")
VectorPolynomialExpand().setSelectedCol("vec").setOutputCol("vec_out").transform(data).collectToDataframe()

```
### Result
| vec            | vec_out                                 |
| -------------- | ---------------------------------------- |
| $8$1:3,2:4,4:7 | $44$2:3.0 4:9.0 5:4.0 7:12.0 8:16.0 14:7.0 16:21.0 17:28.0 19:49.0 |
| $8$2:4,4:5     | $44$5:4.0 8:16.0 14:5.0 17:20.0 19:25.0  |

