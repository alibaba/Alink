# StringIndexer预测

## 功能介绍
基于StringIndexer模型，将一列字符串映射为整数。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 脚本示例
### 脚本代码
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

useLocalEnv(1, config=None)

data = np.array([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

df_data = pd.DataFrame({
    "f0": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='batch')
stream_data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='stream')

stringindexer = StringIndexerTrainBatchOp() \
    .setSelectedCol("f0") \
    .setStringOrderType("frequency_asc")

model = stringindexer.linkFrom(data)

predictor = StringIndexerPredictStreamOp(model).setSelectedCol("f0").setOutputCol("f0_indexed")
predictor.linkFrom(stream_data).print()
StreamOperator.execute()
resetEnv()

```

### 脚本运行结果

```
['f0', 'f0_indexed']
['football', 2]
['football', 2]
['football', 2]
['basketball', 1]
['basketball', 1]
['tennis', 0]
```
