## Description
Accelerated Failure Time Survival Regression.
 Based on the Weibull distribution of the survival time.
 
 (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| censorCol | The value of this column could only be 0 or 1. If the value is 1, it means the event has occurred. | String | ✓ |  |
| quantileProbabilities | Array of quantile probabilities. | double[] |  | [0.01,0.05,0.1,0.25,0.5,0.75,0.9,0.95,0.99] |
| lazyPrintTrainInfoEnabled | Enable lazyPrint of TrainInfo | Boolean |  | false |
| lazyPrintTrainInfoTitle | Title of TrainInfo in lazyPrint | String |  | null |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| epsilon | Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06 | Double |  | 1.0E-6 |
| withIntercept | Whether has intercept or not, default is true | Boolean |  | true |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| l1 | the L1-regularized parameter. | Double |  | 0.0 |
| l2 | the L2-regularized parameter. | Double |  | 0.0 |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
### Code
```python
data = np.array([[1.218, 1.0, "1.560,-0.605"],\
[2.949, 0.0, "0.346,2.158"],\
[3.627, 0.0, "1.380,0.231"],\
[0.273, 1.0, "0.520,1.151"],\
[4.199, 0.0, "0.795,-0.226"]])
df = pd.DataFrame({"label":data[:,0], "censor":data[:,1],"features":data[:,2]})
data = dataframeToOperator(df, schemaStr="label double, censor double, features string",op_type="batch")

reg = AftSurvivalRegression()\
			.setVectorCol("features")\
			.setLabelCol("label")\
			.setCensorCol("censor")\
			.setPredictionCol("result")
pipeline = Pipeline().add(reg)
model = pipeline.fit(data)
model.save().collectToDataframe()
model.transform(data).collectToDataframe()
```

## Results
#### Model

| model_id   | model_info | label_value |
| --- | --- | --- |
| 0          | {"hasInterceptItem":"true","vectorCol":"\"features\"","modelName":"\"AFTSurvivalRegTrainBatchOp\"","labelCol":null,"linearModelType":"\"AFT\"","vectorSize":"3"} | NULL        |
| 1048576    | {"featureColNames":null,"featureColTypes":null,"coefVector":{"data":[2.6373721387804276,-0.49591581739360013,0.19847648151323818,1.5469720551612485]},"coefVectors":null} | NULL        |

#### Prediction
| label      | censor     | features   | pred       |
| --- | --- | --- | --- |
| 0.273      | 1.0        | 0.520,1.151 | 13.571097451777327 |
| 1.218      | 1.0        | 1.560,-0.605 | 5.718263596902868 |
| 3.627      | 0.0        | 1.380,0.231 | 7.380610641992667 |
| 4.199      | 0.0        | 0.795,-0.226 | 9.009354073821902 |
| 2.949      | 0.0        | 0.346,2.158 | 18.067188679653064 |
