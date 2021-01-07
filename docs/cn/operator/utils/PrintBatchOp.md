# 打印数据组件(Batch)

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

data = data = np.array([
       ["0,0,0"],
       ["0.1,0.1,0.1"],
       ["0.2,0.2,0.2"],
       ["9,9,9"],
       ["9.1,9.1,9.1"],
       ["9.2,9.2,9.2"]
])
    
df = pd.DataFrame({"Y": data[:, 0]})

# batch source 
inOp = dataframeToOperator(df, schemaStr='Y string', op_type='batch')



inOp.link(PrintBatchOp())

```

### 脚本运行结果

```
             Y
0        0,0,0
1  0.1,0.1,0.1
2  0.2,0.2,0.2
3        9,9,9
4  9.1,9.1,9.1
5  9.2,9.2,9.2
```
