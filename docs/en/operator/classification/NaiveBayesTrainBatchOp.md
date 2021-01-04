## Description
Naive Bayes classifier is a simple probability classification algorithm using
 Bayes theorem based on independent assumption. It is an independent feature model.
 The input feature can be continual or categorical.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| smoothing | the smoothing factor | Double |  | 0.0 |
| categoricalCols | Names of the categorical columns used for training in the input table | String[] |  |  |
| featureCols | Names of the feature columns used for training in the input table | String[] | ✓ |  |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| weightCol | Name of the column indicating weight | String |  | null |

## Script Example
### Script
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
# train data
batchData = dataframeToOperator(df, schemaStr='f0 double, f1 double, f2 double, f3 double, label int', op_type='batch')

# train op
colnames = ["f0","f1","f2", "f3"]
ns = NaiveBayesTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, batchData).print()
```
### Result

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




