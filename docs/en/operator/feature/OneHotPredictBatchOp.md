## Description
One-hot batch operator maps a serial of columns of category indices to a column of
 sparse binary vectors.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCols | Names of the output columns | String[] |  | null |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "KEEP" |
| encode | encode type: INDEX, VECTOR, ASSEMBLED_VECTOR. | String |  | "ASSEMBLED_VECTOR" |
| dropLast | drop last | Boolean |  | true |
| numThreads | Thread number of operator. | Integer |  | 1 |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    ["a", 1],
    ["b", 1],
    ["c", 1],
    ["e", 2],
    ["a", 2],
    ["b", 1],
    ["c", 2],
    ["d", 2],
    [None, 1]
])

# load data
df = pd.DataFrame({"query": data[:, 0], "label": data[:, 1]})

inOp = dataframeToOperator(df, schemaStr='query string, weight long', op_type='batch')

# one hot train
one_hot = OneHotTrainBatchOp().setSelectedCols(["query"])
model = inOp.link(one_hot)
model.print()

# batch predict
predictor = OneHotPredictBatchOp().setOutputCols(["output"])
print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))
```

#### Results
##### 模型
```python
   column_index                                              token  \
0            -1  {"selectedCols":"[\"query\"]","selectedColType...   
1             0                                                  b   
2             0                                                  c   
3             0                                                  d   
4             0                                                  e   
5             0                                                  a   

   token_index  
0          NaN  
1            0  
2            1  
3            2  
4            3  
5            4
```

##### 预测
```python
  query  weight    output
0     a       1       $5$
1     b       1  $5$0:1.0
2     c       1  $5$1:1.0
3     e       2  $5$3:1.0
4     a       2       $5$
5     b       1  $5$0:1.0
6     c       2  $5$1:1.0
7     d       2  $5$2:1.0
8   NaN       1  $5$4:1.0
```
