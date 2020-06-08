
# TripleToKv

## 功能介绍
将数据格式从 Triple 转成 Kv


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略 | String |  | "ERROR" |
| tripleColumnCol | 三元组结构中列信息的列名 | 三元组结构中列信息的列名 | String | ✓ |  |
| tripleValueCol | 三元组结构中数据信息的列名 | 三元组结构中数据信息的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| kvCol | KV列名 | KV列的列名 | String | ✓ |  |
| kvColDelimiter | 分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  | "," |
| kvValDelimiter | 分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  | ":" |
| tripleRowCol | 三元组结构中行信息的列名 | 三元组结构中行信息的列名 | String |  |  |

## 脚本示例
### 脚本代码
```python
import numpy as np
import pandas as pd


data = np.array([[1,'f1',1.0],[1,'f2',2.0],[2,'f1',4.0],[2,'f2',8.0]])
df = pd.DataFrame({"row":data[:,0], "col":data[:,1], "val":data[:,2]})
data = dataframeToOperator(df, schemaStr="row double, col string, val double",op_type="batch")


op = TripleToKvBatchOp()\
    .setTripleRowCol("row").setTripleColCol("col").setTripleValCol("val")\
    .setReservedCols(["row"]).setKvCol("kv")\
    .linkFrom(data)
op.print()
```

### 脚本运行结果
    
    |row|kv|
    |--|--------------|
    |1|f0:1.0,f1:2.0|
    |2|f0:4.0,f1:8.0|
    
