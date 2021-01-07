# Kafka

## 功能介绍
读Kafka版。Kafka是由Apache软件基金会开发的一个开源流处理平台。详情
请参阅：https://kafka.apache.org/

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| bootstrapServers | bootstrapServers | bootstrapServers | String | ✓ |  |
| groupId | groupId | groupId | String | ✓ |  |
| startupMode | startupMode | startupMode | String | ✓ |  |
| topic | topic名称 | topic名称 | String |  | null |
| topicPattern | "topic pattern" | "topic pattern" | String |  | null |
| startTime | 起始时间 | 起始时间。默认从当前时刻开始读。 | String |  | null |
| properties | 用户自定义Kafka参数 | 用户自定义Kafka参数,形如: "prop1=val1,prop2=val2" | String |  | null |



## 脚本示例

### 脚本代码
```python
data = KafkaSourceStreamOp() \
    .setBootstrapServers("localhost:9092") \
    .setTopic("iris") \
    .setStartupMode("EARLIEST") \
    .setGroupId("alink_group")

data.print()
StreamOperator.execute()
```
