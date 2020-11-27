## Description
Fm classifier pipeline op.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| minibatchSize | mini-batch size | Integer |  | -1 |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTrainInfoEnabled | Enable lazyPrint of TrainInfo | Boolean |  | false |
| lazyPrintTrainInfoTitle | Title of TrainInfo in lazyPrint | String |  | null |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| weightCol | Name of the column indicating weight | String |  | null |
| epsilon | Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06 | Double |  | 1.0E-6 |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| vectorCol | Name of a vector column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
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

#### Code
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
test = dataframeToOperator(df, schemaStr='kv string, label double', op_type='stream')
# load data
fm = FmClassifier().setVectorCol("kv").setLabelCol("label").setPredictionCol("pred")
model = fm.fit(input)
model.transform(test).print()
StreamOperator.execute()
```


#### Result
kv	| label	| pred
---|----|-------
1:1.1 3:2.0|1.0|1.0
2:2.1 10:3.1|1.0|1.0
1:1.2 5:3.2|0.0|0.0
3:1.2 7:4.2|0.0|0.0
