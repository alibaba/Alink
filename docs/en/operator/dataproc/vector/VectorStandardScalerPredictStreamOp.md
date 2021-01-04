## Description
StandardScaler transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example

### Code

```python
data = np.array([\
["a", "10.0, 100"],\
["b", "-2.5, 9"],\
["c", "100.2, 1"],\
["d", "-99.9, 100"],\
["a", "1.4, 1"],\
["b", "-2.2, 9"],\
["c", "100.9, 1"]])
df = pd.DataFrame({"col1":data[:,0], "vec":data[:,1]})
data = dataframeToOperator(df, schemaStr="col1 string, vec string",op_type="batch")
colnames = ["col1", "vec"]
selectedColName = "vec"

trainOp = VectorStandardScalerTrainBatchOp()\
           .setSelectedCol(selectedColName)
       
model = trainOp.linkFrom(data)

#batch predict                  
batchPredictOp = VectorStandardScalerPredictBatchOp()
batchPredictOp.linkFrom(model, data).print()

#stream predict
streamData = dataframeToOperator(df, schemaStr="col1 string, vec string",op_type="stream")

streamPredictOp = VectorStandardScalerPredictStreamOp(trainOp)
streamData.link(streamPredictOp).print()

StreamOperator.execute()
```
### Results

col1|vec
----|---
a|-0.07835182408093559,1.4595814453461897
c|1.2269606224811418,-0.6520885789229323
b|-0.2549018445693762,-0.4814485769617911
a|-0.20280511721213143,-0.6520885789229323
c|1.237090541689495,-0.6520885789229323
b|-0.25924323851581327,-0.4814485769617911
d|-1.6687491397923802,1.4595814453461897



