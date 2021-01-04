## Description
Logistic regression train batch operator. we use log loss func by setting LinearModelType = LR and model
 name = "Logistic Regression".

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

# load data
dataTest = input
colnames = ["f0","f1"]
lr = LogisticRegressionTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = input.link(lr)

predictor = LogisticRegressionPredictBatchOp().setPredictionCol("pred")
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



## 备注

1. 该组件的输入为训练数据，输出为逻辑回归模型。
2. 参数数据库的使用方式可以覆盖多个参数的使用方式。
3. 该组件支持在可视化大屏直接查看模型信息，参见 [模型类组件可视化](https://yuque.antfin-inc.com/pai-user/manual/mqb0xh)。




