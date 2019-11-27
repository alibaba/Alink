# ALS预测

## 功能介绍
ALS预测，可进行评分预测。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
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

#### 脚本运行结果

```
   user  item  rating  predicted_rating
0     1     1     0.6          0.579622
1     2     2     0.8          0.766851
2     2     3     0.6          0.581079
3     4     1     0.6          0.574481
4     4     2     0.3          0.298500
5     4     3     0.4          0.382157
```




