## Description
Make stream prediction based on the multilayer perceptron model fitted by MultilayerPerceptronTrainBatchOp.

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
test_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["f1", "f2"]) \
  .setLabelCol("label") \
  .setLayers([2, 8, 3]) \
  .setMaxIter(10)

model = mlpc.linkFrom(train_data)

predictor = MultilayerPerceptronPredictStreamOp(model)\
  .setPredictionCol('p')

predictor.linkFrom(test_data).print()
StreamOperator.execute()

resetEnv()

```

### Results

```
['f1', 'f2', 'label', 'p']
[0.32157748818958753, 0.49787124043277453, 0, 1]
[0.9988915548832964, 0.8030743157012032, 2, 0]
[0.33727230130595953, 0.81275648997722, 2, 1]
[0.7680458610932143, 0.9917353120756056, 1, 1]
[0.47095782127772334, 0.39421675200119843, 0, 0]
[0.8019966973978703, 0.13171211349239198, 2, 2]
[0.43242357294095524, 0.5696606829395613, 1, 1]
[0.7475103692009955, 0.7353101866795212, 0, 1]
[0.24565789099075186, 0.4938085074750497, 1, 1]
[0.08361045521633703, 0.5737040509691121, 1, 1]
[0.22910529416272918, 0.5867291811070908, 0, 1]
[0.16013950289548107, 0.8000181963308167, 1, 1]
```
