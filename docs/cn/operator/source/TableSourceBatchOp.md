# 从Table中读取BatchOperator数据(Batch)

## 功能介绍
从Table中生成BatchOperator数据

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |



## 脚本示例
### 脚本代码
```python
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [1, "1 1 1"],
    [2, "2 2 2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp.getOutputTable()
TableSourceBatchOp(inOp.getOutputTable()).print()
```
### 运行结果
   id    vec
0   0  0 0 0
1   1  1 1 1
2   2  2 2 2


