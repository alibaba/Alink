## 功能介绍

本算子对输入数据的每个类别进行指定个数的分层随机抽样。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| strataCol | 分层列 | 分层列 | String | ✓ |  |
| strataSize | 采样个数 | 采样个数 | Integer |  | -1 |
| strataSizes | 采样个数 | 采样个数, eg, a:10,b:30 | String | ✓ |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  | false |


## 脚本示例

### 脚本代码

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

data = np.array([
        ['a',0.0,0.0],
        ['a',0.2,0.1],
        ['b',0.2,0.8],
        ['b',9.5,9.7],
        ['b',9.1,9.6],
        ['b',9.3,9.9]
    ])

df_data = pd.DataFrame({
    "x1": data[:, 0],
    "x2": data[:, 1],
    "x3": data[:, 2]
})

batchData = dataframeToOperator(df_data, schemaStr='x1 string, x2 double, x3 double', op_type='batch')
sampleOp = StratifiedSampleWithSizeBatchOp() \
       .setStrataCol("x1") \
       .setStrataSizes("a:1,b:2")

batchData.link(sampleOp).print()
```

### 脚本运行结果

```
  x1   x2   x3
0  a  0.0  0.0
1  b  9.1  9.6
2  b  0.2  0.8
```
