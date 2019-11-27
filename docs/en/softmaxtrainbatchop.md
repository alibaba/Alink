## Description
Softmax is a classifier for multi-class problem.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| optimMethod | optimization method | String |  | null |
| l1 | the L1-regularized parameter. | Double |  | 0.0 |
| l2 | the L2-regularized parameter. | Double |  | 0.0 |
| withIntercept | Whether has intercept or not, default is true | Boolean |  | true |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| epsilon | Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06 | Double |  | 1.0E-6 |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| weightCol | Name of the column indicating weight | String |  | null |
| vectorCol | Name of a vector column | String |  | null |
| standardization | Whether standardize training data or not, default is true | Boolean |  | true |


## Script Example
#### Script
```
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})
batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
dataTest = batchData
colnames = ["f0","f1"]
lr = SoftmaxTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = SoftmaxPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```
#### Result
f0 | f1 | label | pred 
---|----|-------|-----
2|1|1|1
3|2|1|1
4|3|2|2
2|4|1|1
2|2|1|1
4|3|2|2
1|2|1|1
5|3|3|3


