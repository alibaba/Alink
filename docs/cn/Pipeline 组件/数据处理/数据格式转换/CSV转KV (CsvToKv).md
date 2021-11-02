# CSV转KV (CsvToKv)
Java 类名：com.alibaba.alink.pipeline.dataproc.format.CsvToKv

Python 类名：CsvToKv


## 功能介绍
将数据格式从 Csv 转成 Kv


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| csvCol | CSV列名 | CSV列的列名 | String | ✓ |  |
| kvCol | KV列名 | KV列的列名 | String | ✓ |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| csvFieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| kvColDelimiter | 分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  | "," |
| kvValDelimiter | 分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  | ":" |
| quoteChar | 引号字符 | 引号字符 | Character |  | "\"" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
    ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")
 
op = CsvToKv()\
    .setCsvCol("csv")\
    .setSchemaStr("f0 double, f1 double")\
    .setReservedCols(["row"])\
    .setKvCol("kv")\
    .transform(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.format.CsvToKv;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CsvToKvTest {
	@Test
	public void testCsvToKv() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator op = new CsvToKv()
			.setCsvCol("csv")
			.setSchemaStr("f0 double, f1 double")
			.setReservedCols("row")
			.setKvCol("kv")
			.transform(data);
		op.print();
	}
}
```

### 运行结果
    
|row|kv|
|---|---|
|1|f0:1.0,f1:2.0|
|2|f0:4.0,f1:8.0|
    
