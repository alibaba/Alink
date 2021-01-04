## Description
Text Naive Bayes Classifier.

 We support the multinomial Naive Bayes and bernoulli Naive Bayes model, a probabilistic learning method.
 Here, the input data must be vector and the values must be nonnegative.

 Details info of the algorithm:
 https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| modelType | model type : Multinomial or Bernoulli. | String |  | "Multinomial" |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| weightCol | Name of the column indicating weight | String |  | null |
| vectorCol | Name of a vector column | String | ✓ |  |
| smoothing | the smoothing factor | Double |  | 1.0 |

## Script Example
#### Script
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
# train op
ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()

```
#### Result

sv | label | pred
---|-------|----
"$31$0:1.0 1:1.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:1.0 2:0.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0




