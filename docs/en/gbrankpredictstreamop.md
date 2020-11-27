## Description
Gradient Boosting(often abbreviated to GBDT or GBM) is a popular supervised learning model.
 It is the best off-the-shelf supervised learning model for a wide range of problems,
 especially problems with medium to large data size.
 
 This implementation use histogram-based algorithm.
 See:
 "Mcrank: Learning to rank using multiple classification and gradient boosting", Ping Li et al., NIPS 2007,
 for detail and experiments on histogram-based algorithm.
 
 This implementation use layer-wise tree growing strategy,
 rather than leaf-wise tree growing strategy
 (like the one in "Lightgbm: A highly efficient gradient boosting decision tree", Guolin Ke et al., NIPS 2017),
 because we found the former being faster in flink-based distributed computing environment.
 
 This implementation use data-parallel algorithm.
 See:
 "A communication-efficient parallel algorithm for decision tree", Qi Meng et al., NIPS 2016
 for an introduction on data-parallel, feature-parallel, etc., algorithms to construct decision forests.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| vectorCol | Name of a vector column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example

#### Code

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        [2, 1, 0, 0],
        [3, 2, 0, 2],
        [4, 3, 0, 1],
        [2, 4, 0, 0],
        [2, 2, 1, 0],
        [4, 3, 1, 2],
        [1, 2, 1, 1],
        [5, 3, 1, 0]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f0": data[:, 0],
        "f1": data[:, 1],
        "group": data[:, 2],
        "label": data[:, 3]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 double,
    group long, 
    label double
    ''',
        op_type='batch'
    )


def streamSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 double,
    group long, 
    label double
    ''',
        op_type='stream'
    )

trainOp = (
    GBRankBatchOp()
    .setFeatureCols(['f0', 'f1'])
    .setLabelCol("label")
    .setGroupCol("group")
    .linkFrom(batchSource())
)

predictBatchOp = (
    GBRankPredictBatchOp()
    .setPredictionCol('pred')
)

(
    predictBatchOp
    .linkFrom(
        trainOp,
        batchSource()
    )
    .print()
)

predictStreamOp = (
    GBRankPredictStreamOp(
        trainOp
    )
    .setPredictionCol('pred')
)

(
    predictStreamOp
    .linkFrom(
        streamSource()
    )
    .print()
)

StreamOperator.execute()
```

#### Results

```
	f0	f1	group	label	pred
0	2.0	1.0	0	0.0	-10.667265
1	3.0	2.0	0	2.0	12.336884
2	4.0	3.0	0	1.0	4.926796
3	2.0	4.0	0	0.0	-10.816632
4	2.0	2.0	1	0.0	-8.938445
5	4.0	3.0	1	2.0	4.926796
6	1.0	2.0	1	1.0	-1.877382
7	5.0	3.0	1	0.0	-11.284907
```
