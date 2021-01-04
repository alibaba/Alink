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
| alpha | The alpha in implicit preference model. | Double |  | 40.0 |
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

als = AlsImplicitTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
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
1|3^0.5739401578903198 0.7356206178665161 0.2835581600666046 0.4170718789100647 0.5174137949943542 0.16431112587451935 0.49211201071739197 -0.061936844140291214 0.5246134400367737 0.6998568773269653
2|2^3
2|4^1
2|4^2
2|4^3
1|1^0.6226902604103088 0.16985385119915009 -0.08548004180192947 0.6489482522010803 0.4250849187374115 0.7820274829864502 0.28951916098594666 0.8236508369445801 0.3065516948699951 0.28404685854911804
1|2^0.5708359479904175 0.7359125018119812 0.2846967577934265 0.4134806990623474 0.515541672706604 0.15931831300258636 0.4911116361618042 -0.067657470703125 0.5235611796379089 0.6993021965026855
0|1^0.1624351590871811 -0.17678727209568024 -0.16064739227294922 0.23842018842697144 0.06292854249477386 0.41647663712501526 -0.010483191348612309 0.5283675789833069 -0.012455465272068977 -0.09315657615661621
0|2^0.14313405752182007 0.40237587690353394 0.2077043056488037 0.03555755689740181 0.17652447521686554 -0.16940949857234955 0.20788832008838654 -0.3258723318576813 0.22234608232975006 0.34014421701431274
0|4^0.2915375530719757 0.2304707169532776 0.05443307012319565 0.2566309869289398 0.23176348209381104 0.22107504308223724 0.19426916539669037 0.17158512771129608 0.20662355422973633 0.24717546999454498
```

