# KV转三元组 (KvToTripleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.format.KvToTripleBatchOp

Python 类名：KvToTripleBatchOp


## 功能介绍
将数据格式从 Kv 转成 Triple，三元组。一条数据可能输出多条结果。
setTripleColumnValueSchemaStr 设置三元组格式，包含两列，一列为key的值，一般类型为string，如果key为数字字符串，也可以指定为int、long等类型。
一列为value的值，如果指定为数字或者字符串格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| kvCol | KV列名 | KV列的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| tripleColumnValueSchemaStr | 三元组结构中列信息和数据信息的Schema | 三元组结构中列信息和数据信息的Schema | String | ✓ |  |  |
| handleInvalid | 解析异常处理策略 | 解析异常处理策略，可选为ERROR（抛出异常）或者SKIP（输出NULL） | String |  | "ERROR", "SKIP" | "ERROR" |
| kvColDelimiter | 不同key之间分隔符 | 当输入数据为稀疏格式时，key-value对之间的分隔符 | String |  |  | "," |
| kvValDelimiter | key和value之间分隔符 | 当输入数据为稀疏格式时，key和value的分割符 | String |  |  | ":" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | [] |

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
  
op = KvToTripleBatchOp()\
    .setKvCol("kv")\
    .setReservedCols(["row"])\
    .setTripleColumnValueSchemaStr("col string, val double")\
    .linkFrom(data)

op.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.format.KvToTripleBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class KvToTripleBatchOpTest {
	@Test
	public void testKvToTripleBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1", "{\"f0\":\"1.0\",\"f1\":\"2.0\"}", "$3$0:1.0 1:2.0", "f0:1.0,f1:2.0", "1.0,2.0", 1.0, 2.0),
			Row.of("2", "{\"f0\":\"4.0\",\"f1\":\"8.0\"}", "$3$0:4.0 1:8.0", "f0:4.0,f1:8.0", "4.0,8.0", 4.0, 8.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"row string, json string, vec string, kv string, csv string, f0 double, f1 double");
		BatchOperator <?> op = new KvToTripleBatchOp()
			.setKvCol("kv")
			.setReservedCols("row")
			.setTripleColumnValueSchemaStr("col string, val double")
			.linkFrom(data);
		op.print();
	}
}
```

### 运行结果
    
|row|col|val|
    |---|---|---|
    |1|f0|1.0|
    |1|f1|2.0|
    |2|f0|4.0|
    |2|f1|8.0|
