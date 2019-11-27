# ALS训练

## 功能介绍
ALS模型训练，得到user和item两个因子矩阵。

参考文献：
1. explicit feedback: Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007
2. implicit feedback: Collaborative Filtering for Implicit Feedback Datasets, 2008

## 参数说明

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
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  | 10 |<!-- This is the end of auto-generated parameter info -->



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

model = als.linkFrom(data)
model.print()
```

#### 脚本运行结果

```
   user  item                                            factors
0   1.0   NaN  -0.06586061 -0.034223076 0.069877796 0.0920446...
1   2.0   NaN  0.30718762 0.16972417 0.008185322 0.0386066 0....
2   4.0   NaN  -0.06712866 -0.034935225 0.069463015 0.0913517...
3   NaN   1.0  -0.15275586 -0.07944428 0.15982738 0.21034132 ...
4   NaN   2.0  0.5041202 0.27869284 0.01877524 0.07083873 0.3...
5   NaN   3.0  0.23072533 0.12939966 0.06971352 0.118020855 0...
```

