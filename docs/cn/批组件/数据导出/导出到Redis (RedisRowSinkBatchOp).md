# 导出到Redis (RedisRowSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.RedisRowSinkBatchOp

Python 类名：RedisRowSinkBatchOp


## 功能介绍
将一个批式数据，按行写到Redis里，键和值可以是多列。

在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| clusterMode | 集群模式 | 是集群模式还是单机模式 | Boolean |  |  | false |
| databaseIndex | 数据库索引号 | 数据库索引号 | Long |  |  |  |
| keyCols | 多键值列 | 多键值列 | String[] |  |  | null |
| pipelineSize | 流水线大小 | Redis 发送命令流水线的大小 | Integer |  |  | 1 |
| redisIPs | Redis IP | Redis 集群的 IP/端口 | String[] |  |  |  |
| redisPassword | Redis 密码 | Redis 服务器密码 | String |  |  |  |
| timeout | 超时 | 关闭连接的超时时间 | Integer |  |  |  |
| valueCols | 多数值列 | 多数值列 | String[] |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**
	
### Python 代码
```python
redisIP = "127.0.0.1:6379"
		
df = pd.DataFrame([
    ["football", 1.0],
    ["football", 2.0],
    ["football", 3.0],
    ["basketball", 4.0],
    ["basketball", 5.0],
    ["tennis", 6.0],
    ["tennis", 7.0],
    ["pingpang", 8.0],
    ["pingpang", 9.0],
    ["baseball", 10.0]])

batchData = BatchOperator.fromDataframe(df, schemaStr='id string,val double')

batchData.link(RedisRowSinkBatchOp()\
			.setRedisIPs(redisIP)\
			.setKeyCols(["id"])\
			.setValueCols(["val"])\
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

public class RedisRowSinkBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		String redisIP = "127.0.0.1:6379";

		List <Row> df = Arrays.asList(
			Row.of("football", 1.0),
			Row.of("football", 2.0),
			Row.of("football", 3.0),
			Row.of("basketball", 4.0),
			Row.of("basketball", 5.0),
			Row.of("tennis", 6.0),
			Row.of("tennis", 7.0),
			Row.of("pingpang", 8.0),
			Row.of("pingpang", 9.0),
			Row.of("baseball", 10.0)
		);

		BatchOperator <?> data = new MemSourceBatchOp(df, "id string,val double");

		RedisRowSinkBatchOp sink = new RedisRowSinkBatchOp()
			.setPluginVersion("2.9.0")
			.setRedisIPs(redisIP)
			.setKeyCols("id")
			.setValueCols("val");
		
		data.link(sink);

		BatchOperator.execute();
	}

}
```
