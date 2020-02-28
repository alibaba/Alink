# Kafka

## 功能介绍
读Kafka, 支持kafka 1.x和2.x版

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| bootstrapServers | "bootstrap.servers" | "bootstrap.servers" | String | ✓ |  |
| groupId | "group.id" | "group.id" | String | ✓ |  |
| startupMode | "startupMode" | "startupMode", "EARLIEST","GROUP_OFFSETS","LATEST","TIMESTAMP" | String | ✓ |  |
| topic | topic名称 | topic名称 | String |  | null |
| topicPattern | "topic pattern" | "topic pattern" | String |  | null |
| properties | 额外的kafka参数配置 | 额外的kafka参数配置，格式形如"prop1=val1,prop2=val2" | String |  |  |
| startTime | 起始时间 | 起始时间。默认从当前时刻开始读。 | String |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
```python
data = KafkaSourceStreamOp() \
    .setBootstrapServers("localhost:9092") \
    .setTopic("iris") \
    .setStartupMode("EARLIEST") \
    .setGroupId("alink_group")

data.print()
StreamOperator.execute()
```