# Redis表查找 (LookupRedis)
Java 类名：com.alibaba.alink.pipeline.dataproc.LookupRedis

Python 类名：LookupRedis


## 功能介绍
支持数据查找功能，支持多个key的查找，并将查找后的结果中的value列添加到待查询数据后面。

功能类似于 LookUp ，不同的是被查找的数据存储在 Redis 中。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| clusterMode | Not available! | Not available! | Boolean |  |  | false |
| databaseIndex | Not available! | Not available! | Long |  |  |  |
| pipelineSize | Not available! | Not available! | Integer |  |  | 1 |
| redisIPs | Not available! | Not available! | String[] |  |  |  |
| redisPassword | Not available! | Not available! | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| timeout | Not available! | Not available! | Integer |  |  |  |


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

RedisSinkBatchOp()\
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

LookupRedis()\
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
import com.alibaba.alink.operator.batch.sink.RedisSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.LookupRedis;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Collections;

public class LookupRedisTest extends AlinkTestBase {
	@Test
	public void map() throws Exception {
		String redisIP = "*";
		int redisPort = 26379;

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Collections.singletonList(Row.of("id001", 123L, 45.6, "str")),
			"id string, col0 bigint, col1 double, col2 string"
		);

		new RedisSinkBatchOp()
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

		new LookupRedis()
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
