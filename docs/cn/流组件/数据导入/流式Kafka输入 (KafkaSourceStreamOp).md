# 流式Kafka输入 (KafkaSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp

Python 类名：KafkaSourceStreamOp


## 功能介绍
读Kafka版。Kafka是由Apache软件基金会开发的一个开源流处理平台。详情
请参阅：https://kafka.apache.org/

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| bootstrapServers | bootstrapServers | bootstrapServers | String | ✓ |  |
| groupId | groupId | groupId | String | ✓ |  |
| startupMode | startupMode | startupMode | String | ✓ |  |
| properties | 用户自定义Kafka参数 | 用户自定义Kafka参数,形如: "prop1=val1,prop2=val2" | String |  | null |
| startTime | 起始时间 | 起始时间。默认从当前时刻开始读。 | String |  | null |
| topic | topic名称 | topic名称 | String |  | null |
| topicPattern | "topic pattern" | "topic pattern" | String |  | null |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
data = KafkaSourceStreamOp() \
    .setBootstrapServers("localhost:9092") \
    .setTopic("iris") \
    .setStartupMode("EARLIEST") \
    .setGroupId("alink_group")

data.print()
StreamOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp;
import org.junit.Test;

public class KafkaSourceStreamOpTest {
	@Test
	public void testKafkaSourceStreamOp() throws Exception {
		StreamOperator <?> data = new KafkaSourceStreamOp()
			.setBootstrapServers("localhost:9092")
			.setTopic("iris")
			.setStartupMode("EARLIEST")
			.setGroupId("alink_group");
		data.print();
		StreamOperator.execute();
	}
}
```
