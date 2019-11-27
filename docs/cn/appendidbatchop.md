# 附加ID列（batch）
## 功能介绍

将表附加ID列

## 参数说明


<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| appendType | append类型 | append类型，"UNIQUE"和"DENSE"，分别为稀疏和稠密，稀疏的为非连续唯一id，稠密的为连续唯一id | String |  | "DENSE" |
| idCol | ID列名 | ID列名 | String |  | "append_id" |<!-- This is the end of auto-generated parameter info -->


## 脚本示例

#### 脚本代码

```python
import numpy as np
import pandas as pd
from pyalink.alink import *


def exampleData():
    return np.array([
        [1.0, "A", 0, 0, 0],
        [2.0, "B", 1, 1, 0],
        [3.0, "C", 2, 2, 1],
        [4.0, "D", 3, 3, 1]
    ])


def sourceFrame():
    data = exampleData()
    return pd.DataFrame({
        "f0": data[:, 0],
        "f1": data[:, 1],
        "f2": data[:, 2],
        "f3": data[:, 3],
        "label": data[:, 4]
    })


def batchSource():
    return dataframeToOperator(
        sourceFrame(),
        schemaStr='''
    f0 double, 
    f1 string, 
    f2 int, 
    f3 int, 
    label int
    ''',
        op_type='batch'
    )


(
    AppendIdBatchOp()
    .setIdCol("append_id")
    .linkFrom(batchSource())
    .print()
)
```

#### 脚本结果

```
    f0 f1  f2  f3  label  append_id
0  1.0  A   0   0      0          0
1  2.0  B   1   1      0          1
2  3.0  C   2   2      1          2
3  4.0  D   3   3      1          3
```

