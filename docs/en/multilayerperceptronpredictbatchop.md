## Description
Make prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
### Code
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = {
  'f1': np.random.rand(12),
  'f2': np.random.rand(12),
  'label': np.random.randint(low=0, high=3, size=12)
}

df_data = pd.DataFrame(data)
schema = 'f1 double, f2 double, label bigint'
train_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
test_data = train_data

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["f1", "f2"]) \
  .setLabelCol("label") \
  .setLayers([2, 8, 3]) \
  .setMaxIter(10)

model = mlpc.linkFrom(train_data)

predictor = MultilayerPerceptronPredictBatchOp()\
  .setPredictionCol('p')

predictor.linkFrom(model, test_data).print()

resetEnv()

```

### Results

```
          f1        f2  label  p
0   0.398737  0.088554      2  2
1   0.129992  0.025044      1  2
2   0.569760  0.359337      2  2
3   0.672308  0.771075      0  2
4   0.546880  0.436831      2  2
5   0.096233  0.962563      2  2
6   0.567075  0.968310      1  2
7   0.706059  0.998861      0  2
8   0.113452  0.231905      2  2
9   0.686377  0.417600      1  2
10  0.861515  0.288288      2  2
11  0.719469  0.736065      1  2

```
