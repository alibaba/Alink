# Fm 推荐训练

## 功能介绍
Fm 推荐是使用Fm算法在推荐场景的一种扩展，用给定打分数据及user和item的特征信息，训练一个推荐专用的Fm模型，
用于预测user对item的评分、对user推荐itemlist，或者对item推荐userlist。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| userFeatureCols | Not available! | Not available! | String[] |  | [] |
| userCategoricalFeatureCols | Not available! | Not available! | String[] |  | [] |
| itemFeatureCols | Not available! | Not available! | String[] |  | [] |
| itemCategoricalFeatureCols | Not available! | Not available! | String[] |  | [] |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| withLinearItem | 是否含有线性项 | 是否含有线性项 | Boolean |  | true |
| numFactor | 因子数 | 因子数 | Integer |  | 10 |
| lambda0 | 常数项正则化系数 | 常数项正则化系数 | Double |  | 0.0 |
| lambda1 | 线性项正则化系数 | 线性项正则化系数 | Double |  | 0.0 |
| lambda2 | 二次项正则化系数 | 二次项正则化系数 | Double |  | 0.0 |
| numEpochs | epoch数 | epoch数 | Integer |  | 10 |
| learnRate | 学习率 | 学习率 | Double |  | 0.01 |
| initStdev | 初始化参数的标准差 | 初始化参数的标准差 | Double |  | 0.05 |

## 脚本示例
### 脚本代码

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

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

model = FmRecommTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setNumFactor(20)\
    .setRateCol("rating").linkFrom(data);

predictor = FmRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```

### 脚本运行结果
user|	item|	rating|	prediction_result
----|-----|--- |---
1	|1|	0.6|	0.582958
2	|2|	0.8|	0.576914
2	|3|	0.6|	0.508942
4	|1|	0.6|	0.505525
4	|2|	0.3|	0.372908
4	|3|	0.4|	0.347927
