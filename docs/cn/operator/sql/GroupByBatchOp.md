
## 功能介绍
对批数据进行groupby运算


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| groupByPredicate | groupby语句 | groupby语句 | String | ✓ |  |
| selectClause | select语句 | select语句 | String | ✓ |  |


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

batch_data = dataframeToOperator(df_data, schemaStr=schema, op_type='batch')

op = GroupByBatchOp().setGroupByPredicate("f1").setSelectClause("f1,avg(f2) as f2")
batch_data = batch_data.link(op)

batch_data.print()

resetEnv()
```

### 运行结果

```
       f1    f2
0  Nevada  2002
1    Ohio  2001
```
