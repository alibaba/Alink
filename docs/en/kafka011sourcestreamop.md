## Description
Data source for kafka.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| bootstrapServers | kafka集群地址 | String | ✓ |  |
| groupId | 消费组id | String | ✓ |  |
| startupMode | startupMode | String | ✓ |  |
| topic | topic | String |  | null |
| topicPattern | topic pattern | String |  | null |
| startTime | start time | String |  | null |


## Script Example
```python
data = Kafka011SourceStreamOp() \
    .setBootstrapServers("localhost:9092") \
    .setTopic("iris") \
    .setStartupMode("EARLIEST") \
    .setGroupId("alink_group")

data.print()
StreamOperator.execute()
```
