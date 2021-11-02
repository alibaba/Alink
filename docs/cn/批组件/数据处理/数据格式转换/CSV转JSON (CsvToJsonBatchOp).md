# CSV转JSON (CsvToJsonBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.CsvToJsonBatchOp

Python 类名：CsvToJsonBatchOp


## 功能介绍
将数据格式从 Csv 转成 Json


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| csvCol | CSV列名 | CSV列的列名 | String | ✓ |  |
| jsonCol | JSON列名 | JSON列的列名 | String | ✓ |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| csvFieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
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
   
op = CsvToJsonBatchOp()\
    .setCsvCol("csv").setSchemaStr("f0 double, f1 double")\
    .setReservedCols(["row"]).setJsonCol("json")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.CsvToJsonBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CsvToJsonBatchOpTest {
	@Test
	public void testCsvToJsonBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator <?> op = new CsvToJsonBatchOp()
			.setCsvCol("csv").setSchemaStr("f0 double, f1 double")
			.setReservedCols("row").setJsonCol("json")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|json|
|---|----|
| 1 |{"f1":"1.0","f2":"2.0"}|
| 2 |{"f2":"4.0","f4":"8.0"}|
