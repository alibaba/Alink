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
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |
| wordDelimiter | Delimiter of words | String |  | " " |
| predMethod | Method to predict doc vector, support 3 method: avg, min and max, default value is avg. | String |  | "avg" |


## Script Example
#### Code
```
import numpy as np
import pandas as pd

data = np.array([
    ["A B C"]
])

df = pd.DataFrame({"tokens": data[:, 0]})
inOp1 = dataframeToOperator(df, schemaStr='tokens string', op_type='batch')
inOp2 = dataframeToOperator(df, schemaStr='tokens string', op_type='stream')
train = Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4).linkFrom(inOp1)
predictBatch = Word2VecPredictBatchOp().setSelectedCol("tokens").linkFrom(train, inOp1)

[model,predict] = collectToDataframes(train, predictBatch)
print(model)
print(predict)

predictStream = Word2VecPredictStreamOp(train).setSelectedCol("tokens").linkFrom(inOp2)
predictStream.print(refreshInterval=-1)
StreamOperator.execute()
```

#### Results
##### Model
```
rowID word                                                vec
0    C  0.8955382525715048 0.7260255668945033 0.153084...
1    B  0.3799129268855519 0.09451568997723046 0.03543...
2    A  0.9284417086503712 0.7607143212094577 0.417053...
```

##### Prediction
```
rowID    tokens
0  0.7346309627024759 0.5270851926937304 0.201858...
```

