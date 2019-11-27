## Description
Make predictions based on model trained from AlsTrainBatchOp.
 
 There are two types of predictions:
 1) rating prediction: given user and item, predict the rating.
 2) recommend prediction: given a list of users, recommend k items for each users.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
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

als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)
predictor = AlsPredictBatchOp()\
    .setUserCol("user").setItemCol("item").setPredictionCol("predicted_rating")

model = als.linkFrom(data)
predictor.linkFrom(model, data).print()

```

#### Results

```
   user  item  rating  predicted_rating
0     1     1     0.6          0.579622
1     2     2     0.8          0.766851
2     2     3     0.6          0.581079
3     4     1     0.6          0.574481
4     4     2     0.3          0.298500
5     4     3     0.4          0.382157
```





