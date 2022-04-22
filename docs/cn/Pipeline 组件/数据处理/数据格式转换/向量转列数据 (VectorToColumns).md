# 向量转列数据 (VectorToColumns)
Java 类名：com.alibaba.alink.pipeline.dataproc.format.VectorToColumns

Python 类名：VectorToColumns


## 功能介绍
将数据格式从 Vector 转成 Columns


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', '0:1.0,1:2.0', '1.0,2.0', 1.0, 2.0],
    ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', '0:4.0,1:8.0', '4.0,8.0', 4.0, 8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")
 
op = VectorToColumns()\
    .setVectorCol("vec")\
    .setReservedCols(["row"])\
    .setSchemaStr("f0 double, f1 double")\
    .transform(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.format.VectorToColumns;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorToColumnsTest {
	@Test
	public void testVectorToColumns() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "0:1.0,1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator op = new VectorToColumns()
			.setVectorCol("vec")
			.setReservedCols("row")
			.setSchemaStr("f0 double, f1 double")
			.transform(data);
		op.print();
	}
}
```

### 运行结果
    
|row|f0|f1|
|---|---|---|
|1|1.0|2.0|
|2|4.0|8.0|
    
