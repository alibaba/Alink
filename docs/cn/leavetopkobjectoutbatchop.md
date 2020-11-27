# 推荐结果处理

## 功能介绍
将推荐结果按取topK部分作为一个输出表。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| rateThreshold | 打分阈值 | 打分阈值 | Double |  | -Infinity |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ |  |
| groupCol | 分组列 | 分组单列名，必选 | String | ✓ |  |
| objectCol | Object列列名 | Object列列名 | String | ✓ |  |
| fraction | 拆分到测试集最大数据比例 | 拆分到测试集最大数据比例 | Double |  | 1.0 |
| k | 推荐TOP数量 | 推荐TOP数量 | Integer |  | 10 |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |

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
    [4, 0, 0.6],
    [6, 4, 0.3],
    [4, 7, 0.4],
    [2, 6, 0.6],
    [4, 5, 0.6],
    [4, 6, 0.3],
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
spliter = LeaveTopKObjectOutBatchOp()\
			.setK(2)\
			.setGroupCol("user")\
			.setObjectCol("item")\
			.setOutputCol("label")\
            .setRateCol("rating")
spliter.linkFrom(data).print()
spliter.getSideOutput(0).print()

```

### 脚本运行结果
```
	user	label
0	1	{"rating":"[0.6]","object":"[1]"}
1	2	{"rating":"[0.8,0.6]","object":"[2,3]"}
2	4	{"rating":"[0.6,0.6]","object":"[0,5]"}
3	6	{"rating":"[0.3]","object":"[4]"}
        user	item	rating
  0	2	6	0.6
  1	4	7	0.4
  2	4	3	0.4
  3	4	6	0.3
```
