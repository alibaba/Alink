# 前N条数据组件

## 功能介绍
该组件输出表的前N条数据。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| size | 采样个数 | 采样个数 | Integer | ✓ |  |



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

sampleOp = FirstNBatchOp()\
        .setSize(2)

inOp.link(sampleOp).print()

```

### 脚本运行结果

```
             Y
0        0,0,0
1  0.1,0.1,0.1
```
