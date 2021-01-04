## 功能介绍
高斯混合模型聚类

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  | 2 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 当两轮迭代的中心点距离小于epsilon时，算法收敛。 | Double |  | 1.0E-4 |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | 0 |

## 脚本示例
### 脚本代码
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

data = np.array([
    ["-0.6264538 0.1836433"],
    ["-0.8356286 1.5952808"],
    ["0.3295078 -0.8204684"],
    ["0.4874291 0.7383247"],
    ["0.5757814 -0.3053884"],
    ["1.5117812 0.3898432"],
    ["-0.6212406 -2.2146999"],
    ["11.1249309 9.9550664"],
    ["9.9838097 10.9438362"],
    ["10.8212212 10.5939013"],
    ["10.9189774 10.7821363"],
    ["10.0745650 8.0106483"],
    ["10.6198257 9.9438713"],
    ["9.8442045 8.5292476"],
    ["9.5218499 10.4179416"],
])

df_data = pd.DataFrame({
    "features": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='features string', op_type='batch')

gmm = GmmTrainBatchOp() \
    .setVectorCol("features") \
    .setEpsilon(0.)

model = gmm.linkFrom(data)
model.print()
```

### 脚本运行结果

```
   model_id                                         model_info
0         0  {"vectorCol":"\"features\"","numFeatures":"2",...
1   1048576  {"clusterId":0,"weight":0.7354489748549162,"me...
2   2097152  {"clusterId":1,"weight":0.26455102514508383,"m...
```
