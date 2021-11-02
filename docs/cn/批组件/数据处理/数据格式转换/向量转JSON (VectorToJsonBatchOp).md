# 向量转JSON (VectorToJsonBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.VectorToJsonBatchOp

Python 类名：VectorToJsonBatchOp


## 功能介绍
将数据格式从 Vector 转成 Json


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| jsonCol | JSON列名 | JSON列的列名 | String | ✓ |  |
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
    ['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
    ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])

data = BatchOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")
 
op = VectorToJsonBatchOp()\
    .setVectorCol("vec")\
    .setReservedCols(["row"])\
    .setJsonCol("json")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.VectorToJsonBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorToJsonBatchOpTest {
	@Test
	public void testVectorToJsonBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator <?> op = new VectorToJsonBatchOp()
			.setVectorCol("vec")
			.setReservedCols("row")
			.setJsonCol("json")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|json|
|---|----|
| 1 |{"1":"1.0","2":"2.0"}|
| 2 |{"1":"4.0","2":"8.0"}|
