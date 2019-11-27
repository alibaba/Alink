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
| implicitPrefs | Whether to use implicit preference model. | Boolean |  | false |
| alpha | The alpha in implicit preference model. | Double |  | 40.0 |
| numBlocks | Number of blocks when doing ALS. This is a performance parameter. | Integer |  | 1 |
| userCol | User column name | String | ✓ |  |
| itemCol | Item column name | String | ✓ |  |
| rateCol | Rating column name | String | ✓ |  |
| numIter | Number of iterations, The default value is 10 | Integer |  | 10 |
| userCol | User column name | String | ✓ |  |
| itemCol | Item column name | String | ✓ |  |
| predictionCol | Column name of prediction. | String | ✓ |  |


## Script Example
#### Code

```python
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

data = dataframeToOperator(df_data, schemaStr='user bigint, item bigint, rating double', op_type='batch')

als = ALS().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01).setPredictionCol("pred_rating")

pred = als.fit(data).transform(data)
pred.print()
```

#### Results

```
   user  item  rating  pred_rating
0     1     1     0.6     0.579622
1     2     2     0.8     0.766851
2     2     3     0.6     0.581079
3     4     1     0.6     0.574481
4     4     2     0.3     0.298500
5     4     3     0.4     0.382157
```



