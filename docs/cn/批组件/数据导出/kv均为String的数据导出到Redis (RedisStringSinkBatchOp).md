# kv均为String的数据导出到Redis (RedisStringSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.RedisStringSinkBatchOp

Python 类名：RedisStringSinkBatchOp


## 功能介绍
将一个批式数据，（单列String类型键值）按行写到Redis里。
### 注意事项
- 与RedisRowSinkBatchOp不同写入的是序列化后的字符串，例如："A"写入Redis的结果为"A"
- 在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| clusterMode | 集群模式 | 是集群模式还是单机模式 | Boolean |  |  | false |
| databaseIndex | 数据库索引号 | 数据库索引号 | Long |  |  |  |
| keyCol | 单键列 | 单键列 | String |  |  | null |
| pipelineSize | 流水线大小 | Redis 发送命令流水线的大小 | Integer |  |  | 1 |
| redisIPs | Redis IP | Redis 集群的 IP/端口 | String[] |  |  |  |
| redisPassword | Redis 密码 | Redis 服务器密码 | String |  |  |  |
| timeout | 超时 | 关闭连接的超时时间 | Integer |  |  |  |
| valueCol | 单值列 | 单值列 | String |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
redisIP = "*"
redisPort = 0
		
df = pd.DataFrame([
    ["football", "1.0"],
    ["football", "2.0"],
    ["football", "3.0"]])

batchData = BatchOperator.fromDataframe(df, schemaStr='id string,val double')

batchData.link(RedisStringSinkBatchOp()\
			.setRedisIPs(redisIP)\
			.setKeyCol("id")\
			.setValueCol("val")\
			.setPluginVersion("2.9.0"))
			
BatchOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RedisStringSinkBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		String redisIP = "127.0.0.1:6379";
		int redisPort = 0;

		List <Row> df = Arrays.asList(
			Row.of("football", "1.0"),
			Row.of("football", "2.0")
		);

		BatchOperator <?> data = new MemSourceBatchOp(df, "id string,val string");

		RedisStringSinkBatchOp sink = new RedisStringSinkBatchOp()
			.setRedisIPs(redisIP)
			.setKeyCol("id")
			.setValueCol("val")
			.setPluginVersion("2.9.0");

		data.link(sink);

		BatchOperator.execute();
	}

}
```
