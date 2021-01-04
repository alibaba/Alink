## 功能介绍

分层采样是对每个类别进行随机抽样。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| strataCol | 分层列 | 分层列 | String | ✓ |  |
| strataRatio | 采用比率 | 采用比率 | Double |  | -1.0 |
| strataRatios | 采用比率 | 采用比率, eg, a:0.1,b:0.3 | String | ✓ |  |


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

streamData = dataframeToOperator(df_data, schemaStr='x1 string, x2 double, x3 double', op_type='stream')
sampleStreamOp = StratifiedSampleStreamOp()\
       .setStrataCol("x1")\
       .setStrataRatios("a:0.5,b:0.5")

sampleStreamOp.linkFrom(streamData).print()

StreamOperator.execute()
```
### 脚本运行结果

x1|x2|x3
---|---|---
b|9.3|9.9
a|0.0|0.0
b|0.2|0.8



