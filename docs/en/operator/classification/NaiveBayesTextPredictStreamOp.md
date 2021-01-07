## Description
Text Naive Bayes Predictor.

 We support the multinomial Naive Bayes and bernoulli Naive Bayes model, a probabilistic learning method.
 Here, the input data must be vector and the values must be nonnegative.

 Details info of the algorithm:
 https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String | ✓ |  |
| numThreads | Thread number of operator. | Integer |  | 1 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
### Script
```python
data =  np.array([
    ["$31$0:1.0 1:1.0 2:1.0 30:1.0","1.0  1.0  1.0  1.0", '1'],
    ["$31$0:1.0 1:1.0 2:0.0 30:1.0","1.0  1.0  0.0  1.0", '1'],
    ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
    ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
    ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
    ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
    ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0']])

dataSchema = ["sv", "dv", "label"]


df = pd.DataFrame({"sv": data[:, 0], "dv": data[:, 1], "label": data[:, 2]})
# batch data
batchData = dataframeToOperator(df, schemaStr='sv string, dv string, label string', op_type='batch')
# stream data
streamData = dataframeToOperator(df, schemaStr='sv string, dv string, label string', op_type='stream')
# train op
ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesTextPredictStreamOp(model).setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
predictor.linkFrom(streamData).print()
StreamOperator.execute()
```
### Result

sv | label | pred
---|-------|----
"$31$0:1.0 1:1.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:1.0 2:0.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0




