## Description
kafka 011 sink.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| bootstrapServers | kafka集群地址 | String | ✓ |  |
| topic | topic | String | ✓ |  |
| dataFormat | data format | String | ✓ |  |
| fieldDelimiter | Field delimiter | String |  | "," |


## Script Example
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

sink = Kafka011SinkStreamOp() \
    .setBootstrapServers("localhost:9092").setDataFormat("json") \
    .setTopic("iris")
sink.linkFrom(data)
StreamOperator.execute()
```


