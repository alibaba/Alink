# 类型转换 (TypeConvertBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.TypeConvertBatchOp

Python 类名：TypeConvertBatchOp


## 功能介绍

类型转换是用来列类型进行转换的组件

组件可一次性转化多个列到指定的数据类型，但是这些列的数据类型只能为同一种，并且为JDBC Type。

支持的目标类型为 STRING, VARCHAR, FLOAT, DOUBLE, INT, BIGINT, LONG, BOOLEAN。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| targetType | 目标类型 | 转换为的类型，类型应该为JDBC Type。 | String | ✓ |  |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([['Ohio', 2000, 1.5],
                        ['Ohio', 2001, 1.7],
                        ['Ohio', 2002, 3.6],
                        ['Nevada', 2001, 2.4],
                        ['Nevada', 2002, 2.9],
                        ['Nevada', 2003, 3.2],])


batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')
op = TypeConvertBatchOp().setSelectedCols(['f2']).setTargetType('double')
batch_data = batch_data.link(op)
batch_data.print()



```

### 运行结果

```
       f1      f2   f3
0    Ohio  2000.0  1.5
1    Ohio  2001.0  1.7
2    Ohio  2002.0  3.6
3  Nevada  2001.0  2.4
4  Nevada  2002.0  2.9
5  Nevada  2003.0  3.2
```
