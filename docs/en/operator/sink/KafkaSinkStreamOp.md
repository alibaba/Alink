## Description


## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| bootstrapServers | kafka bootstrap servers | String | ✓ |  |
| topic | topic | String | ✓ |  |
| dataFormat | data format | String | ✓ |  |
| fieldDelimiter | Field delimiter | String |  | "," |
| properties | user defined kafka properties, for example: "prop1=val1,prop2=val2" | String |  | null |

## Script Example

### Code

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

