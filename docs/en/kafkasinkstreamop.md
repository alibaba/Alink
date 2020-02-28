## Description
Data sink for kafka 1.x and 2.x

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| bootstrapServers | kafka bootstrap servers | String | ✓ |  |
| topic | topic | String | ✓ |  |
| properties | additional kafka configurations | additional kafka configurations, such as "prop1=val1,prop2=val2" | String |  |  |
| dataFormat | data format | String | ✓ |  |
| fieldDelimiter | Field delimiter | String |  | "," |


## Script Example
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv";
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

sink = KafkaSinkStreamOp() \
    .setBootstrapServers("localhost:9092").setDataFormat("json") \
    .setTopic("iris")
sink.linkFrom(data)
StreamOperator.execute()
```


