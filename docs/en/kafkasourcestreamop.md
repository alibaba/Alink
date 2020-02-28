## Description
Data source for kafka 1.x and 2.x

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| bootstrapServers | kafka bootstrap servers | String | ✓ |  |
| groupId | consumer group id | String | ✓ |  |
| startupMode | startupMode | String | ✓ |  |
| topic | topic | String |  | null |
| topicPattern | topic pattern | String |  | null |
| properties | additional kafka configurations | additional kafka configurations, such as "prop1=val1,prop2=val2" | String |  |  |
| startTime | start time | String |  | null |


## Script Example
```python
data = KafkaSourceStreamOp() \
    .setBootstrapServers("localhost:9092") \
    .setTopic("iris") \
    .setStartupMode("EARLIEST") \
    .setGroupId("alink_group")

data.print()
StreamOperator.execute()
```
