# CSV转向量 (CsvToVectorStreamOp)
Java 类名：com.alibaba.alink.operator.stream.dataproc.format.CsvToVectorStreamOp

Python 类名：CsvToVectorStreamOp


## 功能介绍
将数据格式从 Csv 转成 Vector


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| csvCol | CSV列名 | CSV列的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |  |
| csvFieldDelimiter | 字段分隔符 | 字段分隔符 | String |  |  | "," |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| quoteChar | 引号字符 | 引号字符 | Character |  |  | "\"" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorSize | 向量长度 | 向量长度 | Long |  |  | -1 |

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
     
op = CsvToVectorStreamOp()\
    .setCsvCol("csv")\
    .setSchemaStr("f0 double, f1 double")\
    .setReservedCols(["row"])\
    .setVectorCol("vec")\
    .setVectorSize(5)\
    .linkFrom(data)

op.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.format.CsvToVectorStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CsvToVectorStreamOpTest {
	@Test
	public void testCsvToVectorStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0),
            Row.of("2", "{\"f0\":\"4.0\",\"f1\":\"8.0\"}", "$3$0:4.0 1:8.0", "f0:4.0,f1:8.0", "4.0,8.0", 4.0, 8.0)
		);
		StreamOperator <?> data = new MemSourceStreamOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		StreamOperator <?> op = new CsvToVectorStreamOp()
			.setCsvCol("csv")
			.setSchemaStr("f0 double, f1 double")
			.setReservedCols("row")
			.setVectorCol("vec")
			.setVectorSize(5)
			.linkFrom(data);
		op.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
    
|row|vec|
|---|-----|
|1|$5$1.0 2.0|
|2|$5$4.0 8.0|
    
