# ItemCF 训练

## 功能介绍
ItemCF 是一种被广泛使用的推荐算法，用给定打分数据训练一个推荐模型，
用于预测user对item的评分、对user推荐itemlist，或者对item推荐userlist。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| maxNeighborNumber | Not available! | Not available! | Integer |  | 64 |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| similarityType | 距离度量方式 | 聚类使用的距离类型 | String |  | "COSINE" |
| rateCol | 打分列列名 | 打分列列名 | String |  | null |
| similarityThreshold | Not available! | Not available! | Double |  | 1.0E-4 |

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

model = ItemCfTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRateCol("rating").linkFrom(data);

predictor = ItemCfRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```

### 脚本运行结果
```
	user	item	rating	prediction_result
0	1	1	0.6	0.000000
1	2	2	0.8	0.600000
2	2	3	0.6	0.800000
3	4	1	0.6	0.361237
4	4	2	0.3	0.440631
5	4	3	0.4	0.386137
```
