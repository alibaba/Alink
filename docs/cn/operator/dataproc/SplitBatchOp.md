# 数据集拆分

## 功能介绍
本算子将输入数据按比例拆分为两部分。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| fraction | 拆分到左端的数据比例 | 拆分到左端的数据比例 | Double | ✓ |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | null |



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

spliter = SplitBatchOp().setFraction(0.5)
spliter.linkFrom(batch_data)

spliter.print()

resetEnv()
```

### 脚本运行结果
```
       f1    f2   f3
0    Ohio  2001  1.7
1    Ohio  2002  3.6
2  Nevada  2002  2.9
```
