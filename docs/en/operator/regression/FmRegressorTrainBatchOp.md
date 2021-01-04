## Description
Fm regression train algorithm. the input of this algorithm can be vector or table.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| minibatchSize | mini-batch size | Integer |  | -1 |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| weightCol | Name of the column indicating weight | String |  | null |
| epsilon | Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06 | Double |  | 1.0E-6 |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| withIntercept | Whether has intercept or not, default is true | Boolean |  | true |
| withLinearItem | with linear item. | Boolean |  | true |
| numFactor | number of factor | Integer |  | 10 |
| lambda0 | lambda0 | Double |  | 0.0 |
| lambda1 | lambda1 | Double |  | 0.0 |
| lambda2 | lambda_2 | Double |  | 0.0 |
| numEpochs | num epochs | Integer |  | 10 |
| learnRate | learn rate | Double |  | 0.01 |
| initStdev | init stdev | Double |  | 0.05 |

## Script Example
#### Script
```python
import numpy as np
import pandas as pd
data = np.array([
    ["1:1.1 3:2.0", 1.0],
    ["2:2.1 10:3.1", 1.0],
    ["1:1.2 5:3.2", 0.0],
    ["3:1.2 7:4.2", 0.0]])
df = pd.DataFrame({"kv": data[:, 0], 
                   "label": data[:, 1]})

input = dataframeToOperator(df, schemaStr='kv string, label double', op_type='batch')
# load data
dataTest = input
fm = FmRegressorTrainBatchOp().setVectorCol("kv").setLabelCol("label")
model = input.link(fm)

predictor = FmRegressorPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```
#### Result
kv	| label	| pred
---|----|-------
1:1.1 3:2.0|1.0|0.473600
2:2.1 10:3.1|1.0|0.755115
1:1.2 5:3.2|0.0|0.005875
3:1.2 7:4.2|0.0|0.004641

## 备注
该组件的输入为训练数据，输出为Fm回归模型。




