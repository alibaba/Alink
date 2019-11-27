## Description
MinMaxScaler transforms a dataSet of rows, rescaling each feature
 to a specific range [min, max). (often [0, 1]).
 MinMaxScalerTrain will train a model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| min | Lower bound after transformation. | Double |  | 0.0 |
| max | Upper bound after transformation. | Double |  | 1.0 |


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
trainOp = VectorMinMaxScalerTrainBatchOp()\
           .setSelectedCol("vec")
model = trainOp.linkFrom(data) 

batchPredictOp = VectorMinMaxScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).collectToDataframe()
```
#### Result

col1|vec
----|---
a|0.5473107569721115,1.0
b|0.4850597609561753,0.08080808080808081
c|0.9965139442231076,0.0
d|0.0,1.0
a|0.5044820717131474,0.0
b|0.4865537848605578,0.08080808080808081
c|1.0,0.0




