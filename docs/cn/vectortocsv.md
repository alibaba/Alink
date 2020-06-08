
# VectorToCsv

## 功能介绍
将数据格式从 Vector 转成 Csv


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略 | String |  | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| csvCol | CSV列名 | CSV列的列名 | String | ✓ |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| csvFieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |
| quoteChar | 引号字符 | 引号字符 | Character |  | "\"" |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |

## 脚本示例
### 脚本代码
```python
import numpy as np
import pandas as pd


data = np.array([['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

df = pd.DataFrame({"row":data[:,0], "json":data[:,1], "vec":data[:,2], "kv":data[:,3], "csv":data[:,4], "f0":data[:,5], "f1":data[:,6]})
data = dataframeToOperator(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double",op_type="batch")
    

op = VectorToCsv()\
    .setVectorCol("vec")\
    .setReservedCols(["row"]).setCsvCol("csv").setSchemaStr("f0 double, f1 double")\
    .transform(data)
op.print()
```

### 脚本运行结果
    
|row|csv|
|-|-------|
|1|1.0,2.0|
|2|4.0,8.0|
    
