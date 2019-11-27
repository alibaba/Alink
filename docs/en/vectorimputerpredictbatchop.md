## Description
Imputer completes missing values in a dataSet, but only same type of columns can be selected at the same time.
 Imputer Train will train a model for predict.
 Strategy support min, max, mean or value.
 If min, will replace missing value with min of the column.
 If max, will replace missing value with max of the column.
 If mean, will replace missing value with mean of the column.
 If value, will replace missing value with the value.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| outputCol | Name of the output column | String |  | null |


## Script Example

#### Script
``` python
data = np.array([["1:3,2:4,4:7", 1],\
    ["1:3,2:NaN", 3],\
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="batch")
vecFill = VectorImputerTrainBatchOp().setSelectedCol("vec")
model = data.link(vecFill)
VectorImputerPredictBatchOp().setOutputCol("vec1").linkFrom(model, data).collectToDataframe()
```
#### Result


| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:3.0 2:4.0 4:7.0 |
| 1:3,2:NaN   | 3    | 1:3.0 2:4.0       |
| 2:4,4:5     | 4    | 2:4.0 4:5.0       |
