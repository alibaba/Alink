
## 功能介绍

- 本算子是按照数据点的权重对数据按照比例进行加权采样，权重越大的数据点被采样的可能性越大。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| weightCol | 权重列名 | 权重列对应的列名 | String | ✓ |  |
| ratio | 采样比例 | 采样率，范围为[0, 1] | Double | ✓ |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  | false |


## 脚本示例

### 脚本代码
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    ["a", 1.3, 1.1],
    ["b", 2.5, 0.9],
    ["c", 100.2, -0.01],
    ["d", 99.9, 100.9],
    ["e", 1.4, 1.1],
    ["f", 2.2, 0.9],
    ["g", 100.9, -0.01],
    ["j", 99.5, 100.9],
])

df = pd.DataFrame({"id": data[:, 0], "weight": data[:, 1], "value": data[:, 2]})

# batch source
inOp = dataframeToOperator(df, schemaStr='id string, weight double, value double', op_type='batch')
sampleOp = WeightSampleBatchOp() \
  .setWeightCol("weight") \
  .setRatio(0.5) \
  .setWithReplacement(False)

inOp.link(sampleOp).print()
```
### 结果
```
  id  weight   value
0  g   100.9   -0.01
1  d    99.9  100.90
2  c   100.2   -0.01
3  j    99.5  100.90
```








