## Description
StandardScaler transforms a dataSet, normalizing each feature to have unit standard deviation and/or zero mean.
 If withMean is false, set mean as 0; if withStd is false, set std as 1.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| withMean | Centers the data with mean before scaling. | Boolean |  | true |
| withStd | Scales the data to unit standard deviation. true by default | Boolean |  | true |

## Script Example

### Code

```python
data = np.array([["a", "10.0, 100"],\
    ["b", "-2.5, 9"],\
    ["c", "100.2, 1"],\
    ["d", "-99.9, 100"],\
    ["a", "1.4, 1"],\
    ["b", "-2.2, 9"],\
    ["c", "100.9, 1"]])
df = pd.DataFrame({"col" : data[:,0], "vector" : data[:,1]})
data = dataframeToOperator(df, schemaStr="col string, vector string",op_type="batch")
trainOp = VectorStandardScalerTrainBatchOp().setSelectedCol("vector")
model = trainOp.linkFrom(data)
VectorStandardScalerPredictBatchOp().linkFrom(model, data).collectToDataframe()
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


