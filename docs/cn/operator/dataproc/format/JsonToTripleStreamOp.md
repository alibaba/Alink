
# JsonToTriple

## 功能介绍
将数据格式从 Json 转成 Triple


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略 | String |  | "ERROR" |
| tripleColumnValueSchemaStr | 三元组结构中列信息和数据信息的Schema | 三元组结构中列信息和数据信息的Schema | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | [] |
| jsonCol | JSON列名 | JSON列的列名 | String | ✓ |  |

## 脚本示例
### 脚本代码
```python
import numpy as np
import pandas as pd


data = np.array([['1', '{"f1":"1.0","f2":"2.0"}', '$3$1:1.0 2:2.0', '1:1.0,2:2.0', '1.0,2.0', 1.0, 2.0],
['2', '{"f2":"4.0","f4":"8.0"}', '$3$1:4.0 2:8.0', '1:4.0,2:8.0', '4.0,8.0', 4.0, 8.0]])

df = pd.DataFrame({"row":data[:,0], "json":data[:,1], "vec":data[:,2], "kv":data[:,3], "csv":data[:,4], "f0":data[:,5], "f1":data[:,6]})
data = dataframeToOperator(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double",op_type="stream")
    

op = JsonToTripleStreamOp()\
    .setJsonCol("json")\
    .setReservedCols(["row"]).setTripleColumnValueSchemaStr("col string, val double")\
    .linkFrom(data)
op.print()
StreamOperator.execute()
```

### 脚本运行结果
    
    |row|col|val|
    |-|-|---|
    |1|1|1.0|
    |1|2|2.0|
    |2|1|4.0|
    |2|2|8.0|
