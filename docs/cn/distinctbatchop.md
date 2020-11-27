## 功能介绍
数据去重

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |




## 脚本示例
### 脚本代码

```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

data = {
  'f1': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada', 'Nevada'],
}

df_data = pd.DataFrame(data)
schema = 'f1 string'

batch_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')
op = DistinctBatchOp()
batch_data = batch_data.link(op)
batch_data.print()

resetEnv()
```

### 运行结果

```
       f1
0  Nevada
1    Ohio
```
