# 向量转KV (VectorToKvStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.format.VectorToKvStreamOp

Python 类名：VectorToKvStreamOp


## 功能介绍
将数据格式从 Vector 转成 Kv，流式组件

一条输入数据对应一条输出数据，setKvCol设置kv输出列名，setReservedCols设置保留的输入列。
在输出kv列中，vector的下标对应key，vector的值对应的value。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| kvCol | KV列名 | KV列的列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| kvColDelimiter | 不同key之间分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  |  | "," |
| kvValDelimiter | key和value之间分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  |  | ":" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
    ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

data = StreamOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")

op = VectorToKvStreamOp()\
    .setVectorCol("vec")\
    .setReservedCols(["row"])\
    .setKvCol("kv")\
    .linkFrom(data)

op.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.format.VectorToKvStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorToKvStreamOpTest {
	@Test
	public void testVectorToKvStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0),
			Row.of("2", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:4.0 1:8.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		StreamOperator <?> data = new MemSourceStreamOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		StreamOperator <?> op = new VectorToKvStreamOp()
			.setVectorCol("vec")
			.setReservedCols("row")
			.setKvCol("kv")
			.linkFrom(data);
		op.print();
		StreamOperator.execute();
	}
}
```

### 运行结果

row|kv
---|---
1|0:1.0,1:2.0
2|0:4.0,1:8.0
    
