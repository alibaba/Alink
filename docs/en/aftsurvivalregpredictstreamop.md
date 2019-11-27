## Description
Accelerated Failure Time Survival Regression.
 Based on the Weibull distribution of the survival time.
 
 (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| quantileProbabilities | Array of quantile probabilities. | double[] |  | [0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99] |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |


## Script Example
#### Code
```python
data = np.array([[1.218, 1.0, "1.560,-0.605"],\
[2.949, 0.0, "0.346,2.158"],\
[3.627, 0.0, "1.380,0.231"],\
[0.273, 1.0, "0.520,1.151"],\
[4.199, 0.0, "0.795,-0.226"]])
df = pd.DataFrame({"label":data[:,0], "censor":data[:,1],"features":data[:,2]})
data = dataframeToOperator(df, schemaStr="label double, censor double, features string",op_type="batch")
dataStream = dataframeToOperator(df, schemaStr="label double, feature double",op_type="stream")
trainOp = AftSurvivalRegTrainBatchOp()\
				.setVectorCol("features")\
					.setLabelCol("label")\
				.setCensorCol("censor")
model = trainOp.linkFrom(data)
predictOp = AftSurvivalRegPredictStreamOp(model).setPredictionCol("pred")
res = predictOp.linkFrom(dataStream)
res.print()
StreamOperator.execute()
```

#### Results
##### Model

| model_id   | model_info | label_value |
| --- | --- | --- |
| 0          | {"hasInterceptItem":"true","vectorCol":"\"features\"","modelName":"\"AFTSurvivalRegTrainBatchOp\"","labelCol":null,"linearModelType":"\"AFT\"","vectorSize":"3"} | NULL        |
| 1048576    | {"featureColNames":null,"featureColTypes":null,"coefVector":{"data":[2.6373721387804276,-0.49591581739360013,0.19847648151323818,1.5469720551612485]},"coefVectors":null} | NULL        |

##### Prediction
| label      | censor     | features   | pred       |
| --- | --- | --- | --- |
| 0.273      | 1.0        | 0.520,1.151 | 13.571097451777327 |
| 1.218      | 1.0        | 1.560,-0.605 | 5.718263596902868 |
| 3.627      | 0.0        | 1.380,0.231 | 7.380610641992667 |
| 4.199      | 0.0        | 0.795,-0.226 | 9.009354073821902 |
| 2.949      | 0.0        | 0.346,2.158 | 18.067188679653064 |
