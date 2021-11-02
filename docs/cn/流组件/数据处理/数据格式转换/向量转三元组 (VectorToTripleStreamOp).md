# 向量转三元组 (VectorToTripleStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.format.VectorToTripleStreamOp

Python 类名：VectorToTripleStreamOp


## 功能介绍
将数据格式从 Vector 转成 Triple


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| tripleColumnValueSchemaStr | 三元组结构中列信息和数据信息的Schema | 三元组结构中列信息和数据信息的Schema | String | ✓ |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | [] |

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

op = VectorToTripleStreamOp()\
    .setVectorCol("vec")\
    .setReservedCols(["row"])\
    .setTripleColumnValueSchemaStr("col string, val double")\
    .linkFrom(data)

op.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.format.VectorToTripleStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorToTripleStreamOpTest {
	@Test
	public void testVectorToTripleStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		StreamOperator <?> data = new MemSourceStreamOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		StreamOperator <?> op = new VectorToTripleStreamOp()
			.setVectorCol("vec")
			.setReservedCols("row")
			.setTripleColumnValueSchemaStr("col string, val double")
			.linkFrom(data);
		op.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
    
|row|col|val|
|---|---|---|
|1|1|1.0|
|1|2|2.0|
|2|1|4.0|
|2|2|8.0|
