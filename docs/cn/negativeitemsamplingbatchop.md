# CTR 负采样

## 功能介绍
当给定user-item pair数据的时候，为数据生成若干负样本数据，构成训练数据。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| samplingFactor | Not available! | Not available! | Integer |  | 3 |

## 脚本示例
### 脚本代码

```python
from pyalink.alink import *
import pandas as pd
import numpy as np


data = np.array([
    [1, 1],
    [2, 2],
    [2, 3],
    [4, 1],
    [4, 2],
    [4, 3],
])

df_data = pd.DataFrame({
    "user": data[:, 0],
    "item": data[:, 1]
})
df_data["user"] = df_data["user"].astype('int')
df_data["item"] = df_data["item"].astype('int')

schema = 'user bigint, item bigint'
data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

NegativeItemSamplingBatchOp().linkFrom(data).print()
```

### 脚本运行结果
```
	user	item	label
0	2	1	0
1	1	1	1
2	2	1	0
3	1	2	0
4	2	1	0
5	2	2	1
6	2	1	0
7	2	1	0
8	4	2	1
9	4	3	1
10	1	3	0
11	2	1	0
12	2	3	1
13	4	1	1
14	1	2	0
```
