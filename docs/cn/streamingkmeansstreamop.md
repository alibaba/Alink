## 功能介绍

流式KMeans聚类，需要三种输入: 
1。训练好的批式的KMeans模型
2。流式的更新模型的数据
3。流式的需要预测的数据

组件会根据2流入的数据在固定的timeinterval内更新模型，这个模型会用来预测3的输入数据。



## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| halfLife | 半生命周期 | 半生命周期 | Integer | ✓ |  |
| predictionDistanceCol | 预测距离列名 | 预测距离列名 | String |  |  |
| predictionClusterCol | 预测距离列名 | 预测距离列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| timeInterval | 时间间隔 | 时间间隔，单位秒 | Long | ✓ |  |





## 脚本示例

### 脚本代码

```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)
data = np.array([
  [0, "0 0 0"],
  [1, "0.1,0.1,0.1"],
  [2, "0.2,0.2,0.2"],
  [3, "9 9 9"],
  [4, "9.1 9.1 9.1"],
  [5, "9.2 9.2 9.2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})

inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
stream_data = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')

kmeans = KMeansTrainBatchOp().setVectorCol("vec").setK(2)
init_model = kmeans.linkFrom(inOp)

streamingkmeans = StreamingKMeansStreamOp(init_model) \
  .setTimeInterval(1) \
  .setHalfLife(1) \
  .setReservedCols(["vec"])

pred = streamingkmeans.linkFrom(stream_data, stream_data)

resetEnv()

```
