# Redis 表查找String类型 (LookupRedisStringBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.LookupRedisStringBatchOp

Python 类名：LookupRedisStringBatchOp


## 功能介绍
支持数据查找功能，查找每个key在Redis对应的value。
功能类似于 LookUpBatchOp ，不同的是被查找的数据存储在 Redis 中，
可以和RedisStringSinkBatchOp或RedisStringSinkStreamOp组件配合使用，也可以查找其他方式保存到Redis中的String类型数据。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| clusterMode | 集群模式 | 是集群模式还是单机模式 | Boolean |  |  | false |
| databaseIndex | 数据库索引号 | 数据库索引号 | Long |  |  |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| pipelineSize | 流水线大小 | Redis 发送命令流水线的大小 | Integer |  |  | 1 |
| redisIPs | Redis IP | Redis 集群的 IP/端口 | String[] |  |  |  |
| redisPassword | Redis 密码 | Redis 服务器密码 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| timeout | 超时 | 关闭连接的超时时间 | Integer |  |  |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    ["id001", 123, 45.6, "str"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id string, col0 bigint, col1 double, col2 string')

redisIP = "*:*"

RedisStringSinkBatchOp()\
	.setRedisIPs([redisIP])\
	.setKeyCols(["id"])\
	.setPluginVersion("2.9.0")\
	.setValueCols(["col0", "col1", "col2"])\
	.linkFrom(inOp)

BatchOperator.execute()

df2 = pd.DataFrame([
    ["id001"]
])
needToLookup = BatchOperator.fromDataframe(df2, schemaStr="id string")

LookupRedisStringBatchOp()\
	.setRedisIPs([redisIP])\
	.setPluginVersion("2.9.0") \
    .setKeyCol("id") \
    .setValueCol("col2") \
    .setOutputSchemaStr("col0 bigint, col1 double, col2 string")\
	.linkFrom(needToLookup)\
	.lazyPrint(10)

BatchOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.LookupRedisStringBatchOp;
import com.alibaba.alink.operator.batch.sink.RedisStringSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Collections;

public class LookupRedisStringBatchOpTest extends AlinkTestBase {
	@Test
	public void map() throws Exception {
		String redisIP = "*:*";

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Collections.singletonList(Row.of("id001", 123L, 45.6, "str")),
			"id string, col0 bigint, col1 double, col2 string"
		);

		new RedisStringSinkBatchOp()
			.setRedisIPs(redisIP)
			.setKeyCol("id")
            .setValueCol("col2")
			.setPluginVersion("2.9.0")
			.linkFrom(memSourceBatchOp);

		BatchOperator.execute();

		MemSourceBatchOp needToLookup = new MemSourceBatchOp(
			Collections.singletonList(Row.of("id001")),
			"id string"
		);

		new LookupRedisStringBatchOp()
            .setRedisIPs(redisIP)
            .setPluginVersion("2.9.0")
            .setSelectedCol("id")
            .setOutputCol("col2")
			.linkFrom(needToLookup)
			.print();

		BatchOperator.execute();
	}

}
```

### 运行结果
| id    | col2 |
|----|------|
| id001 | str  |
