## Description
Naive Bayes Classifier.

 We support the multinomial Naive Bayes and multinomial Naive Bayes model, a probabilistic learning method.
 Here, feature values of train table must be nonnegative.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| modelType | model type : Multinomial or Bernoulli. | String |  | "Multinomial" |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| weightCol | Name of the column indicating weight | String |  | null |
| vectorCol | Name of a vector column | String |  | null |
| smoothing | the smoothing factor | Double |  | 1.0 |


## Script Example
#### Script
```python
data = np.array([
    [1.0, 1.0, 0.0, 1.0, 1],
    [1.0, 0.0, 1.0, 1.0, 1],
    [1.0, 0.0, 1.0, 1.0, 1],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [1.0, 1.0, 1.0, 1.0, 1],
    [0.0, 1.0, 1.0, 0.0, 0]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "f2": data[:, 2],
                   "f3": data[:, 3],
                   "label": data[:, 4]})
df["label"] = df["label"].astype('int')
batchData = dataframeToOperator(df, schemaStr='f0 double, f1 double, f2 double, f3 double, label int', op_type='batch')

# load data
colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)

predictor = NaiveBayesPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```
#### Result

f0 | f1 | f2 | f3 | label | pred
---|----|----|----|-------|----
1.0|1.0|0.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
1.0|1.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0





