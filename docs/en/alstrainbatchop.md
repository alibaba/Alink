## Description
Matrix factorization using Alternating Least Square method.
 
 ALS tries to decompose a matrix R as R = X * Yt. Here X and Y are called factor matrices.
 Matrix R is usually a sparse matrix representing ratings given from users to items.
 ALS tries to find X and Y that minimize || R - X * Yt ||^2. This is done by iterations.
 At each step, X is fixed and Y is solved, then Y is fixed and X is solved.
 
 The algorithm is described in "Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007"
 
 We also support implicit preference model described in
 "Collaborative Filtering for Implicit Feedback Datasets, 2008"

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| rank | Rank of the factorization (>0). | Integer |  | 10 |
| lambda | regularization parameter (>= 0). | Double |  | 0.1 |
| nonnegative | Whether enforce the non-negative constraint. | Boolean |  | false |
| numBlocks | Number of blocks when doing ALS. This is a performance parameter. | Integer |  | 1 |
| userCol | User column name | String | ✓ |  |
| itemCol | Item column name | String | ✓ |  |
| rateCol | Rating column name | String | ✓ |  |
| numIter | Number of iterations, The default value is 10 | Integer |  | 10 |

## Script Example
### Code

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    [1, 1, 0.6],
    [2, 2, 0.8],
    [2, 3, 0.6],
    [4, 1, 0.6],
    [4, 2, 0.3],
    [4, 3, 0.4],
])

df_data = pd.DataFrame({
    "user": data[:, 0],
    "item": data[:, 1],
    "rating": data[:, 2],
})
df_data["user"] = df_data["user"].astype('int')
df_data["item"] = df_data["item"].astype('int')

schema = 'user bigint, item bigint, rating double'
data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)

model = als.linkFrom(data)
model.print()

resetEnv()

```

### Results

```
f1|f2
--|--
-1|["uid BIGINT,factors VARCHAR","iid BIGINT,factors VARCHAR","uid BIGINT,iid BIGINT"]
2|1^1
2|2^2
1|3^0.2492007315158844 0.15098488330841064 0.012884462252259254 0.23749813437461853 0.18824179470539093 0.2330566942691803 0.14502321183681488 0.21603597700595856 0.15502716600894928 0.17380373179912567
2|2^3
2|4^1
2|4^2
2|4^3
1|1^0.3039683699607849 0.06367748230695724 0.018810953944921494 0.2667402923107147 0.20158863067626953 0.40229329466819763 0.18074487149715424 0.3677302896976471 0.17684338986873627 0.16401298344135284
1|2^0.29378432035446167 0.27262595295906067 0.012758990749716759 0.2980150878429413 0.24392834305763245 0.1820652037858963 0.16794554889202118 0.17283867299556732 0.19238688051700592 0.24258670210838318
0|1^0.26144763827323914 0.05291144549846649 0.01622731238603592 0.2290731519460678 0.17295707762241364 0.3478386700153351 0.15552067756652832 0.3178976774215698 0.15191656351089478 0.1403297632932663
0|2^0.3697585463523865 0.31101226806640625 0.016883453354239464 0.36896517872810364 0.2995398938655853 0.2606053352355957 0.21240323781967163 0.24531401693820953 0.2388727366924286 0.292529821395874
0|4^0.21535639464855194 0.04601389169692993 0.013304134830832481 0.1891522854566574 0.1430312991142273 0.28413689136505127 0.12802591919898987 0.25975263118743896 0.12538200616836548 0.11655863374471664
```

