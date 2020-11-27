## Description
VectorSlicer is a transformer that takes a feature vector and outputs a new feature vector with a sub-array of the
 original features. It is useful for extracting features from a vector column.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| indices | indices of a vector to be sliced | int[] |  | null |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| outputCol | Name of the output column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example

### Script
```
data = np.array([["1:3,2:4,4:7", 1],
    ["0:3,5:5", 3],
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="stream")   
vecSlice = VectorSliceStreamOp().setSelectedCol("vec").setOutputCol("vec_slice").setIndices([1,2,3])
vecSlice.linkFrom(data).print()
StreamOperator.execute()
```
### Result

| vec         | id   | vec_slice      |
| ----------- | ---- | -------------- |
| 1:3,2:4,4:7 | 1    | $3$0:3.0 1:4.0 |
| 0:3,5:5     | 3    | $3$            |
| 2:4,4:5     | 4    | $3$1:4.0       |
