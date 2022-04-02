# 向量转列数据 (VectorToColumnsStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.format.VectorToColumnsStreamOp

Python 类名：VectorToColumnsStreamOp


## 功能介绍
将数据格式从 Vector 转成 Columns，vector中的数据拆分成多列

一条输入对应一条输出结果，输入的vector可以为稀疏格式，也可以为稠密格式。
vector的数据维度不需要保持一致。

setReservedCols设置保留的输入列。
设置SchemaStr时需要注意，字段个数必须小于等于vector列的最小维度。字段名称和类型用空格分隔，类型必须为double。
当vector维度大于SchemaStr中的字段个数时，输入vector中后面的维度会被忽略。如果vector在前面维度没有值，默认输出为0.0。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ['1', '{"f0":"1.0","f1":"2.0"}', '$4$0:1.0 3:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
    ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 2:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

data = StreamOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")

op = VectorToColumnsStreamOp()\
    .setVectorCol("vec")\
    .setReservedCols(["row"])\
    .setSchemaStr("f0 double, f1 double, f2 double")\
    .linkFrom(data)

op.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.format.VectorToColumnsStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorToColumnsStreamOpTest {
	@Test
	public void testVectorToColumnsStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$4$0:1.0 3:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0),
			Row.of("2", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:4.0 2:8.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		StreamOperator <?> data = new MemSourceStreamOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		StreamOperator <?> op = new VectorToColumnsStreamOp()
			.setVectorCol("vec")
			.setReservedCols("row")
			.setSchemaStr("f0 double, f1 double, f2 double")
			.linkFrom(data);
		op.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

row|f0|f1|f2
---|---|---|---
1|1.0000|0.0000|0.0000
2|4.0000|0.0000|8.0000
    
