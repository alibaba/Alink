# ALS相似item实时推荐

## 功能介绍
使用ALS (Alternating Lease Square）model 对相似的item的进行实时推荐。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  | 10 |
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
sdata = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)

model = als.linkFrom(data)
alsRec = AlsSimilarItemsRecommender().setModelData(model) \
    .setItemCol("item").setRecommCol("rec").setK(1).setReservedCols(["item"])

alsRec.transform(data).print();
```

### 脚本运行结果

item| rec
----|-------
1	|{"object":"[3]","score":"[0.8821980357170105]"}
2	|{"object":"[3]","score":"[0.9917739629745483]"}
3	|{"object":"[2]","score":"[0.9917739629745483]"}
1	|{"object":"[3]","score":"[0.8821980357170105]"}
2	|{"object":"[3]","score":"[0.9917739629745483]"}
3	|{"object":"[2]","score":"[0.9917739629745483]"}
