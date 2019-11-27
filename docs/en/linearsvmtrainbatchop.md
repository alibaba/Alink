## Description
Linear svm train batch operator. it uses hinge loss func by setting LinearModelType = SVM and model name = "linear
 SVM".

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| C | the penalty item parameter. | Double |  | 1.0 |
| optimMethod | optimization method | String |  | null |
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
```python
import numpy as np
import pandas as pd
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 2]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

input = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
dataTest = input
colnames = ["f0","f1"]
svm = LinearSvmTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = input.link(svm)

predictor = LinearSvmPredictBatchOp().setPredictionCol("pred")
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
5|3|2|2



