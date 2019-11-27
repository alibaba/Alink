## Description
Isotonic regression.
 Implement using parallelized pool adjacent violators algorithm.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| featureCol | Name of the feature column。 | String |  | null |
| isotonic | If true, the output sequence should be increasing! | Boolean |  | true |
| featureIndex | Feature index in the vector. | Integer |  | 0 |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| weightCol | Name of the column indicating weight | String |  | null |
| vectorCol | Name of a vector column | String |  | null |


## Script Example
#### Code
```python
data = np.array([[0.35, 1],\
[0.6, 1],\
[0.55, 1],\
[0.5, 1],\
[0.18, 0],\
[0.1, 1],\
[0.8, 1],\
[0.45, 0],\
[0.4, 1],\
[0.7, 0],\
[0.02, 1],\
[0.3, 0],\
[0.27, 1],\
[0.2, 0],\
[0.9, 1]])

df = pd.DataFrame({"feature" : data[:,0], "label" : data[:,1]})
data = dataframeToOperator(df, schemaStr="label double, feature double",op_type="batch")

trainOp = IsotonicRegTrainBatchOp()\
            .setFeatureCol("feature")\
			.setLabelCol("label")
model = trainOp.linkFrom(data)
predictOp = IsotonicRegPredictBatchOp().setPredictionCol("result")
predictOp.linkFrom(model, data).collectToDataframe()
```

#### Results
##### Model
| model_id   | model_info |
| --- | --- |
| 0          | {"vectorCol":"\"col2\"","featureIndex":"0","featureCol":null} |
| 1048576    | [0.02,0.3,0.35,0.45,0.5,0.7] |
| 2097152    | [0.5,0.5,0.6666666865348816,0.6666666865348816,0.75,0.75] |
##### Prediction
| col1       | col2       | col3       | pred       |
| --- | --- | --- | --- |
| 1.0        | 0.9        | 1.0        | 0.75       |
| 0.0        | 0.7        | 1.0        | 0.75       |
| 1.0        | 0.35       | 1.0        | 0.6666666865348816 |
| 1.0        | 0.02       | 1.0        | 0.5        |
| 1.0        | 0.27       | 1.0        | 0.5        |
| 1.0        | 0.5        | 1.0        | 0.75       |
| 0.0        | 0.18       | 1.0        | 0.5        |
| 0.0        | 0.45       | 1.0        | 0.6666666865348816 |
| 1.0        | 0.8        | 1.0        | 0.75       |
| 1.0        | 0.6        | 1.0        | 0.75       |
| 1.0        | 0.4        | 1.0        | 0.6666666865348816 |
| 0.0        | 0.3        | 1.0        | 0.5        |
| 1.0        | 0.55       | 1.0        | 0.75       |
| 0.0        | 0.2        | 1.0        | 0.5        |
| 1.0        | 0.1        | 1.0        | 0.5        |

