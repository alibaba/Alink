# 打印数据组件(Stream)

## 功能介绍
该组件打印表中数据。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |




## 脚本示例

### 脚本代码

```python
import numpy as np
import pandas as pd

data = np.array([
    [0, "abcde", "aabce"],
    [1, "aacedw", "aabbed"],
    [2, "cdefa", "bbcefa"],
    [3, "bdefh", "ddeac"],
    [4, "acedm", "aeefbc"]
])
df = pd.DataFrame({"id": data[:, 0], "text1": data[:, 1], "text2": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='id long, text1 string, text2 string', op_type='stream')

inOp.link(PrintStreamOp())
StreamOperator.execute()

```

### 脚本运行结果

```
id  text1  text2
0  abcde  aabce
1  aacedw  aabbed
2  cdefa  bbcefa
3  bdefh  ddeac
4  acedm  aeefbc
```
