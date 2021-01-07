
## 功能介绍
对列进行重命名

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |



## 脚本示例
### 脚本代码

```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

data = {
  'f1': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada', 'Nevada'],
  'f2': [2000, 2001, 2002, 2001, 2002, 2003],
  'f3': [1.5, 1.7, 3.6, 2.4, 2.9, 3.2]
}

df_data = pd.DataFrame(data)
schema = 'f1 string, f2 bigint, f3 double'

stream_data = dataframeToOperator(df_data, schemaStr=schema, op_type='stream')
op = AsStreamOp().setClause("ff1,ff2,ff3")
stream_data = stream_data.link(op)
stream_data.print()
StreamOperator.execute()

resetEnv()

```

### 脚本运行结果
```
['ff1', 'ff2', 'ff3']
['Ohio', 2000, 1.5]
['Ohio', 2001, 1.7]
['Ohio', 2002, 3.6]
['Nevada', 2001, 2.4]
['Nevada', 2002, 2.9]
['Nevada', 2003, 3.2]
```
