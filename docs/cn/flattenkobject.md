# 推荐结果流式处理

## 功能介绍
将流式推荐结果从json序列化格式转为table格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |
| outputColTypes | 输出结果列列类型数组 | 输出结果列类型数组，必选 | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 脚本示例
### 脚本代码

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

data = np.array([
   [1,'{"rating":"[0.6]","object":"[1]"}'],
   [2,'{"rating":"[0.8,0.6]","object":"[2,3]"}'],
   [3,'{"rating":"[0.6,0.3,0.4]","object":"[1,2,3]"}']
])

df_data = pd.DataFrame({
    "user": data[:, 0],
    "rec": data[:, 1]
})
df_data["user"] = df_data["user"].astype('int')
df_data["rec"] = df_data["rec"].astype('str')

schema = 'user bigint, rec string'
jsonData = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')

recList = FlattenKObject()\
			.setSelectedCol("rec")\
			.setOutputColTypes(["long","double"])\
			.setReservedCols(["user"])\
			.setOutputCols(["object", "rating"])\
			.transform(jsonData).print();
StreamOperator.execute();
```

### 脚本运行结果
```
	user	object	rating
0	1	1	0.6
1	2	2	0.8
2	2	3	0.6
3	3	1	0.6
4	3	2	0.3
5	3	3	0.4
```
