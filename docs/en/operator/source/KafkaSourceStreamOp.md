## Description


## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| bootstrapServers | kafka bootstrap servers | String | ✓ |  |
| groupId | group id | String | ✓ |  |
| startupMode | startupMode | String | ✓ |  |
| topic | topic | String |  | null |
| topicPattern | topic pattern | String |  | null |
| startTime | start time | String |  | null |
| properties | user defined kafka properties, for example: "prop1=val1,prop2=val2" | String |  | null |

## Script Example

### Code
```python
data = KafkaSourceStreamOp() \
    .setBootstrapServers("localhost:9092") \
    .setTopic("iris") \
    .setStartupMode("EARLIEST") \
    .setGroupId("alink_group")

data.print()
StreamOperator.execute()
```
