## 功能介绍
ALS模型训练，得到user和item两个因子矩阵。

参考文献：
1. explicit feedback: Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007
2. implicit feedback: Collaborative Filtering for Implicit Feedback Datasets, 2008


## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| rank | 因子数 | 因子数 | Integer |  | 10 |
| lambda | 正则化系数 | 正则化系数 | Double |  | 0.1 |
| nonnegative | 是否约束因子非负 | 是否约束因子非负 | Boolean |  | false |
| implicitPrefs | 是否采用隐式偏好模型 | 是否采用隐式偏好模型 | Boolean |  | false |
| alpha | 隐式偏好模型系数alpha | 隐式偏好模型系数alpha | Double |  | 40.0 |
| numBlocks | 分块数目 | 分块数目 | Integer |  | 1 |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ |  |
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  | 10 |
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

als = ALS().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01).setPredictionCol("pred_rating")

pred = als.fit(data).transform(data)
pred.print()
```

#### 脚本运行结果

```
   user  item  rating  pred_rating
0     1     1     0.6     0.579622
1     2     2     0.8     0.766851
2     2     3     0.6     0.581079
3     4     1     0.6     0.574481
4     4     2     0.3     0.298500
5     4     3     0.4     0.382157
```


