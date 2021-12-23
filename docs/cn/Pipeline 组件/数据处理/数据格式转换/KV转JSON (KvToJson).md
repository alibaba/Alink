# KV转JSON (KvToJson)
Java 类名：com.alibaba.alink.pipeline.dataproc.format.KvToJson

Python 类名：KvToJson


## 功能介绍
将数据格式从 Kv 转成 Json


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| jsonCol | JSON列名 | JSON列的列名 | String | ✓ |  |
| kvCol | KV列名 | KV列的列名 | String | ✓ |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR" |
| kvColDelimiter | 分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  | "," |
| kvValDelimiter | 分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  | ":" |
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
 
op = KvToJson()\
    .setKvCol("kv")\
    .setReservedCols(["row"])\
    .setJsonCol("json")\
    .transform(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.format.KvToJson;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KvToJsonTest {
	@Test
	public void testKvToJson() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator op = new KvToJson()
			.setKvCol("kv")
			.setReservedCols("row")
			.setJsonCol("json")
			.transform(data);
		op.print();
	}
}
```

### 运行结果
    
|row|json|
|---|----|
| 1 |{"f0":"1.0","f1":"2.0"}|
| 2 |{"f0":"4.0","f1":"8.0"}|
