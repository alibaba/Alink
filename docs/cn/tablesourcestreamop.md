# 从Table中读取StreamOperator数据(Stream)

## 功能介绍
从Table中生成StreamOperator数据

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
inOp = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
inOp.getOutputTable()
TableSourceStreamOp(inOp.getOutputTable()).print()
StreamOperator.execute()
```
### 运行结果
   id    vec
  0  0 0 0
  1  1 1 1
  2  2 2 2


