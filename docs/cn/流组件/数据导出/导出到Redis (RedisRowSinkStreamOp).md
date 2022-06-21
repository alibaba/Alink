# 导出到Redis (RedisRowSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.RedisRowSinkStreamOp

Python 类名：RedisRowSinkStreamOp


## 功能介绍
将一个流式数据，按行写到Redis里，键和值可以是多列。

在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| clusterMode | Not available! | Not available! | Boolean |  |  | false |
| databaseIndex | Not available! | Not available! | Long |  |  |  |
| keyCols | 多键值列 | 多键值列 | String[] |  |  | null |
| pipelineSize | Not available! | Not available! | Integer |  |  | 1 |
| redisIPs | Not available! | Not available! | String[] |  |  |  |
| redisPassword | Not available! | Not available! | String |  |  |  |
| timeout | Not available! | Not available! | Integer |  |  |  |
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

streamData = StreamOperator.fromDataframe(df, schemaStr='id string,val double')

streamData.link(RedisRowSinkStreamOp()\
			.setRedisIPs(redisIP)\
			.setKeyCols(["id"])\
			.setValueCols(["val"])\
			.setPluginVersion("2.9.0"))
			
StreamOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RedisRowSinkStreamOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		String redisIP = "*";
		int redisPort = 0;

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

		StreamOperator <?> data = new MemSourceStreamOp(df, "id string,val double");

		RedisRowSinkStreamOp sink = new RedisRowSinkStreamOp()
			.setRedisIPs(redisIP)
			.setKeyCols("id")
			.setValueCols("val")
			.setPluginVersion("2.9.0");

		data.link(sink);

		StreamOperator.execute();
	}

}
```
