## Description
VectorEleWiseProduct multiplies each input vector by a provided “scaling” vector, using element-wise multiplication.
 In other words, it scales each column of the dataset by a scalar multiplier. This represents the Hadamard product
 between the input vector, v and transforming vector, w, to yield a result vector.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| scalingVector | scaling vector with str format | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example

### Script
```
data = np.array([["1:3,2:4,4:7", 1],\
    ["0:3,5:5", 3],\
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="stream")
vecEP = VectorElementwiseProductStreamOp().setSelectedCol("vec") \
	.setOutputCol("vec1") \
	.setScalingVector("$8$1:3.0 3:3.0 5:4.6")
data.link(vecEP).print()
StreamOperator.execute()
```
### Result
| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:9.0 2:0.0 4:0.0 |
| 0:3,5:5     | 3    | 0:0.0 5:23.0      |
| 2:4,4:5     | 4    | 2:0.0 4:0.0       |
