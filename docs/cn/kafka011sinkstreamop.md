# Kafka011

## 功能介绍
写Kafka 0.11版

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| bootstrapServers | "bootstrap.servers" | "bootstrap.servers" | String | ✓ |  |
| topic | topic名称 | topic名称 | String | ✓ |  |
| dataFormat | 数据格式 | 数据格式。json或csv | String | ✓ |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
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

