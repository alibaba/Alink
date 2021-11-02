# 全表统计 (SummarizerBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.SummarizerBatchOp

Python 类名：SummarizerBatchOp


## 功能介绍

全表统计用来计算整表的统计量，包含count, sum, variance等 

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1, 1,2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2,2.0, False],
    ["c", 0, 0, 0.0, False]
])
source = BatchOperator.fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

summarizer = SummarizerBatchOp()\
    .setSelectedCols(["f_long", "f_int", "f_double"])

summary = summarizer.linkFrom(source).collectSummary()

print(summary.sum('f_double'))
```
### 运行结果

```
1.0
```
