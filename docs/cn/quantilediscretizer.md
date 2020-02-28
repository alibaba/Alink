# 分位点离散

## 功能介绍

分位点离散可以计算选定列的分位点，然后使用这些分位点进行离散化。
生成选中列对应的q-quantile，其中可以所有列指定一个，也可以每一列对应一个

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  | null |
| leftOpen | 左开右闭 | 左开右闭 | Boolean | | true |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| handleInvalid | 未知Token处理策略 | 未知Token处理策略，"keep", "skip", "error" | String | | "keep" |
| encode | 编码方式 | 编码方式，"INDEX", "VECTOR", "ASSEMBLED_VECTOR" | String |   |INDEX |
| dropLast | 是否删除最后一个元素 | 是否删除最后一个元素 | Boolean |  | true |

<!-- This is the end of auto-generated parameter info -->

## 脚本示例

#### 脚本代码

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        ["a", 1, 1, 2.0, True],
        ["c", 1, 2, -3.0, True],
        ["a", 2, 2, 2.0, False],
        ["c", 0, 0, 0.0, False]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f_string": data[:, 0],
        "f_long": data[:, 1],
        "f_int": data[:, 2],
        "f_double": data[:, 3],
        "f_boolean": data[:, 4]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
    f_double double, 
    f_boolean boolean
    ''',
        op_type='batch'
    )

def streamSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f_string string, 
    f_long long, 
    f_int int, 
    f_double double, 
    f_boolean boolean
    ''',
        op_type='stream'
    )

(
    QuantileDiscretizer()
    .setSelectedCols(['f_double'])
    .setNumBuckets(8)
    .fit(batchSource())
    .transform(batchSource())
    .print()
)

(
    QuantileDiscretizer()
    .setSelectedCols(['f_double'])
    .setNumBuckets(8)
    .fit(batchSource())
    .transform(streamSource())
    .print()
)

StreamOperator.execute()
```

#### 脚本结果
批预测结果
```
  f_string  f_long  f_int  f_double  f_boolean
0        a       1      1         2       True
1        c       1      2         0       True
2        a       2      2         2      False
3        c       0      0         1      False
```
流预测结果
```
	f_string	f_long	f_int	f_double	f_boolean
0	c	1	2	0	True
1	c	0	0	1	False
2	a	1	1	2	True
3	a	2	2	2	False
```
