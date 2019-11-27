## Description
MaxAbsScaler transforms a dataSet of Vector rows, rescaling each feature to range
 [-1, 1] by dividing through the maximum absolute value in each feature.
 MaxAbsScalerTrain will train a model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |


## Script Example

#### Script

```python
data = np.array([["a", "10.0, 100"],\
    ["b", "-2.5, 9"],\
    ["c", "100.2, 1"],\
    ["d", "-99.9, 100"],\
    ["a", "1.4, 1"],\
    ["b", "-2.2, 9"],\
    ["c", "100.9, 1"]])
df = pd.DataFrame({"col" : data[:,0], "vec" : data[:,1]})
data = dataframeToOperator(df, schemaStr="col string, vec string",op_type="batch")
trainOp = VectorMaxAbsScalerTrainBatchOp()\
           .setSelectedCol("vec")
model = trainOp.linkFrom(data) 

batchPredictOp = VectorMaxAbsScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).collectToDataframe()
```
#### Result

col1|vec
----|---
c|1.0,0.01
b|-0.024777006937561942,0.09
d|-0.9900891972249752,1.0
a|0.09910802775024777,1.0
b|-0.02180376610505451,0.09
c|0.9930624380574826,0.01
a|0.013875123885034686,0.01




