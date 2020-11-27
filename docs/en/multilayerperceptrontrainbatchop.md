## Description
MultilayerPerceptronClassifier is a neural network based multi-class classifier.
 Valina neural network with all dense layers are used, the output layer is a softmax layer.
 Number of inputs has to be equal to the size of feature vectors.
 Number of outputs has to be equal to the total number of labels.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| layers | Size of each neural network layers. | int[] | ✓ |  |
| blockSize | Size for stacking training samples, the default value is 64. | Integer |  | 64 |
| initialWeights | Initial weights. | DenseVector |  | null |
| vectorCol | Name of a vector column | String |  | null |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| epsilon | Convergence tolerance for iterative algorithms (>= 0), The default value is 1.0e-06 | Double |  | 1.0E-6 |
| l1 | the L1-regularized parameter. | Double |  | 0.0 |
| l2 | the L2-regularized parameter. | Double |  | 0.0 |

## Script Example
### Code
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = {
  'f1': np.random.rand(8),
  'f2': np.random.rand(8),
  'label': np.random.randint(low=1, high=3, size=8)
}

df_data = pd.DataFrame(data)
schema = 'f1 double, f2 double, label bigint'
train_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["f1", "f2"]) \
  .setLabelCol("label") \
  .setLayers([2, 8, 3]) \
  .setMaxIter(10)

mlpc.linkFrom(train_data).print()
resetEnv()

```

### Results

```
           model_id                                         model_info  label_value
0                 0  {"vectorCol":null,"isVectorInput":"false","lay...          NaN
1           1048576  {"data":[2.330192598639656,1.895916109869876,1...          NaN
2  2251799812636672                                                NaN            1
3  2251799812636673                                                NaN            2
```
