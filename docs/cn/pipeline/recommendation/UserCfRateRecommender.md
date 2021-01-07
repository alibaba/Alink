# UserCF 打分

## 功能介绍
UserCF 打分是使用UserCF模型，预测user对item的评分。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

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

model = UserCfTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRateCol("rating").linkFrom(data);

predictor = UserCfRateRecommender()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result")\
    .setModelData(model)

predictor.transform(data).print()
```

### 脚本运行结果
```
    user	item	rating	prediction_result
0	1	1	0.6	0.6
1	2	2	0.8	0.3
2	2	3	0.6	0.4
3	4	1	0.6	0.6
4	4	2	0.3	0.8
5	4	3	0.4	0.6
```
