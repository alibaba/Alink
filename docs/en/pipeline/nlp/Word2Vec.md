## Description
Word2vec is a group of related models that are used to produce word embeddings.
 These models are shallow, two-layer neural networks that are trained to reconstruct
 linguistic contexts of words.

 reference:
 https://en.wikipedia.org/wiki/Word2vec
 Mikolov, Tomas; et al. (2013). "Efficient Estimation of Word Representations in Vector Space"
 Mikolov, Tomas; Sutskever, Ilya; Chen, Kai; Corrado, Greg S.; Dean, Jeff (2013).
 Distributed representations of words and phrases and their compositionality.
 https://code.google.com/archive/p/word2vec/

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lazyPrintTrainInfoEnabled | Enable lazyPrint of TrainInfo | Boolean |  | false |
| lazyPrintTrainInfoTitle | Title of TrainInfo in lazyPrint | String |  | null |
| numIter | Number of iterations, The default value is 1 | Integer |  | 1 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| vectorSize | vector size of embedding | Integer |  | 100 |
| alpha | learning rate of sgd | Double |  | 0.025 |
| wordDelimiter | Delimiter of words | String |  | " " |
| minCount | minimum count of word | Integer |  | 5 |
| randomWindow | Is random window or not | String |  | "true" |
| window | the length of window in w2v | Integer |  | 5 |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |
| wordDelimiter | Delimiter of words | String |  | " " |
| predMethod | Method to predict doc vector, support 3 method: avg, min and max, default value is avg. | String |  | "AVG" |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
#### Code
```
import numpy as np
import pandas as pd

data = np.array([
    ["A B C"]
])

df = pd.DataFrame({"tokens": data[:, 0]})
inOp = dataframeToOperator(df, schemaStr='tokens string', op_type='batch')
word2vec = Word2Vec().setSelectedCol("tokens").setMinCount(1).setVectorSize(4)
word2vec.fit(inOp).transform(inOp).print()
```

#### Results
##### Prediction
```
rowID    tokens
0  0.7346309627024759 0.5270851926937304 0.201858...
```
