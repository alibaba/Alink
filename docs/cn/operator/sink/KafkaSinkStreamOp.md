# Kafka

## 功能介绍
写Kafka Plugin版。Kafka是由Apache软件基金会开发的一个开源流处理平台。详情
            请参阅：https://kafka.apache.org/

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| bootstrapServers | bootstrapServers | bootstrapServers | String | ✓ |  |
| topic | topic名称 | topic名称 | String | ✓ |  |
| dataFormat | 数据格式 | 数据格式。json,csv | String | ✓ |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |
| properties | 用户自定义Kafka参数 | 用户自定义Kafka参数,形如: "prop1=val1,prop2=val2" | String |  | null |



## 脚本示例

### 脚本代码

```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv";
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

sink = KafkaSinkStreamOp() \
    .setBootstrapServers("localhost:9092").setDataFormat("json") \
    .setTopic("iris")
sink.linkFrom(data)
StreamOperator.execute()
```

