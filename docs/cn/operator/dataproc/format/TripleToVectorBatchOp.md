
# TripleToVector

## 功能介绍
将数据格式从 Triple 转成 Vector


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略 | String |  | "ERROR" |
| tripleColumnCol | 三元组结构中列信息的列名 | 三元组结构中列信息的列名 | String | ✓ |  |
| tripleValueCol | 三元组结构中数据信息的列名 | 三元组结构中数据信息的列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| vectorSize | 向量长度 | 向量长度 | Long |  | -1 |
| tripleRowCol | 三元组结构中行信息的列名 | 三元组结构中行信息的列名 | String |  | null |

## 脚本示例
### 脚本代码
```python
import numpy as np
import pandas as pd


data = np.array([[1,'1',1.0],[1,'2',2.0]])
df = pd.DataFrame({"row":data[:,0], "col":data[:,1], "val":data[:,2]})
data = dataframeToOperator(df, schemaStr="row double, col string, val double",op_type="batch")


op = TripleToVectorBatchOp()\
    .setTripleRowCol("row").setTripleColumnCol("col").setTripleValueCol("val")\
    .setVectorCol("vec").setVectorSize(5)\
    .linkFrom(data)
op.print()
```

### 脚本运行结果
    
    |row|vec|
    |-|-----|
    |1|$5$1.0 2.0|
    
