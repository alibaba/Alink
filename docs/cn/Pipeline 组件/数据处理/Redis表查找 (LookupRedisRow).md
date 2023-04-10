# Redis表查找 (LookupRedisRow)
Java 类名：com.alibaba.alink.pipeline.dataproc.LookupRedisRow

Python 类名：LookupRedisRow


## 功能介绍
支持数据查找功能，支持多个key的查找，并将查找后的结果中的value列添加到待查询数据后面。
需要和RedisRowSinkBatchOp或RedisRowSinkStreamOp组件配合使用，该组件用来保存数据。

功能类似于 LookUp ，不同的是被查找的数据存储在 Redis 中。

## 参数说明

<!-- PARAMETER TABLE -->


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    ["id001", 123, 45.6, "str"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id string, col0 bigint, col1 double, col2 string')

redisIP = "*"
redisPort = 26379

RedisRowSinkBatchOp()\
	.setRedisIP(redisIP)\
	.setRedisPort(redisPort)\
	.setKeyCols(["id"])\
	.setPluginVersion("2.9.0")\
	.setValueCols(["col0", "col1", "col2"])\
	.linkFrom(inOp)

BatchOperator.execute()

df2 = pd.DataFrame([
    ["id001"]
])
needToLookup = StreamOperator.fromDataframe(df2, schemaStr="id string")

LookupRedisRow()\
	.setRedisIP(redisIP)\
	.setRedisPort(redisPort)\
	.setPluginVersion("2.9.0")\
	.setSelectedCols(["id"])\
	.setOutputSchemaStr("col0 bigint, col1 double, col2 string")\
	.transform(needToLookup)\
	.print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.RedisRowSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.LookupRedisRow;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Collections;

public class LookupRedisRowTest extends AlinkTestBase {
	@Test
	public void map() throws Exception {
		String redisIP = "*";
		int redisPort = 26379;

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Collections.singletonList(Row.of("id001", 123L, 45.6, "str")),
			"id string, col0 bigint, col1 double, col2 string"
		);

		new RedisRowSinkBatchOp()
			.setRedisIP(redisIP)
			.setRedisPort(redisPort)
			.setKeyCols("id")
			.setPluginVersion("2.9.0")
			.setValueCols("col0", "col1", "col2")
			.linkFrom(memSourceBatchOp);

		BatchOperator.execute();

		MemSourceStreamOp needToLookup = new MemSourceStreamOp(
			Collections.singletonList(Row.of("id001")),
			"id string"
		);

		new LookupRedisRow()
			.setRedisIP(redisIP)
			.setRedisPort(redisPort)
			.setPluginVersion("2.9.0")
			.setSelectedCols("id")
			.setOutputSchemaStr("col0 bigint, col1 double, col2 string")
			.transform(needToLookup)
			.print();

		StreamOperator.execute();
	}

}
```

### 运行结果
| id    | col0 |    col1 | col2 |
|-------+------+---------+------|
| id001 |  123 | 45.6000 | str  |