## Description
KNN classifier is to classify unlabeled observations by assigning them to the class of the most similar
 labeled examples.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| k | k | Integer |  | 10 |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| featureCols | Names of the feature columns used for training in the input table | String[] |  | null |
| vectorCol | Name of a vector column | String |  | null |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| distanceType | Distance type for clustering | String |  | "EUCLIDEAN" |
| predictionCol | Column name of prediction. | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example

#### Code
```python
from pyalink.alink import *
import numpy as np
import pandas as pd

useLocalEnv(2)
data = np.array([
  [1, "0,0,0"],
  [1, "0.1,0.1,0.1"],
  [1, "0.2,0.2,0.2"],
  [0, "9,9,9"],
  [0, "9.1,9.1,9.1"],
  [0, "9.2,9.2,9.2"]
])

df = pd.DataFrame({"label": data[:, 0], "vec": data[:, 1]})
dataSource = dataframeToOperator(df, schemaStr="label int, vec string", op_type='batch')

knn = KnnClassifier().setVectorCol("vec") \
    .setPredictionCol("pred") \
    .setLabelCol("label") \
    .setK(3)

model = knn.fit(dataSource)
model.transform(dataSource).print()
```


#### Result
```
   label          vec  pred
0      1        0,0,0     1
1      1  0.1,0.1,0.1     1
2      1  0.2,0.2,0.2     1
3      0        9,9,9     0
4      0  9.1,9.1,9.1     0
5      0  9.2,9.2,9.2     0
```
