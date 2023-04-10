# 分位数 (QuantileBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.QuantileBatchOp

Python 类名：QuantileBatchOp


## 功能介绍

[quantile](https://en.wikipedia.org/wiki/Quantile)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| quantileNum | 分位个数 | 分位个数 | Integer | ✓ | x >= 0 |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| roundMode | 取整的模式 | 取整的模式。当q是组的个数，k 是第k个组，total是总的样本大小时，第k组的边界索引应为(1.0 / q) * (total - 1) * k。这个值应该为整数，所以需要取整，取整时用到这个参数。 | String |  | "CEIL", "FLOOR", "ROUND" | "ROUND" |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
        [1.0, 0],
        [2.0, 1],
        [3.0, 2],
        [4.0, 3]
    ])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 double')
StreamData = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 double')

QuantileBatchOp()\
    .setSelectedCol("f0")\
    .setQuantileNum(100)\
    .linkFrom(batchData)\
    .print()
    
QuantileStreamOp()\
    .setSelectedCols(["f0"])\
    .setQuantileNum(100)\
    .linkFrom(StreamData)\
    .print()   
    
StreamOperator.execute()
```

### 运行结果

```
      f0  quantile
0    1.0         0
1    1.0         1
2    1.0         2
3    1.0         3
4    1.0         4
..   ...       ...
96   4.0        96
97   4.0        97
98   4.0        98
99   4.0        99
100  4.0       100

[101 rows x 2 columns]
```
