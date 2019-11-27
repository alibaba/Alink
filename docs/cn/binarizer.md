## 功能介绍
给定一个阈值，将连续变量二值化。

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| threshold | 二值化阈值 | 二值化阈值 | Double |  | 0.0 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 脚本代码
```python
# -*- coding=UTF-8 -*-
import numpy as np
import pandas as pd
data = np.array([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])
df = pd.DataFrame({"double": data[:, 0], "bool": data[:, 1], "number": data[:, 2], "str": data[:, 3]})

inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
binarizer = Binarizer().setSelectedCol("double").setThreshold(2.0)
binarizer.transform(inOp).print()
```
#### 脚本运行结果

##### 输出数据
```
rowID   double   bool  number str
0     0.0   True       2   A
1     0.0  False       2   B
2     0.0   True       1   B
3     1.0   True       1   A
```
