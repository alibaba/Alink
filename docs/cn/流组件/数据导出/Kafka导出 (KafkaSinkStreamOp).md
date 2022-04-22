# Kafka导出 (KafkaSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp

Python 类名：KafkaSinkStreamOp


## 功能介绍
写Kafka Plugin版。Kafka是由Apache软件基金会开发的一个开源流处理平台。详情
            请参阅：https://kafka.apache.org/

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| bootstrapServers | bootstrapServers | bootstrapServers | String | ✓ |  |  |
| dataFormat | 数据格式 | 数据格式。json,csv | String | ✓ | "JSON", "CSV" |  |
| topic | topic名称 | topic名称 | String | ✓ |  |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  |  | "," |
| properties | 用户自定义Kafka参数 | 用户自定义Kafka参数,形如: "prop1= val1, prop2 = val2" | String |  |  | null |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

sink = KafkaSinkStreamOp() \
    .setBootstrapServers("localhost:9092").setDataFormat("json") \
    .setTopic("iris")
sink.linkFrom(data)
StreamOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.KafkaSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class KafkaSinkStreamOpTest {
	@Test
	public void testKafkaSinkStreamOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		StreamOperator <?> data = new CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		StreamOperator <?> sink = new KafkaSinkStreamOp()
			.setBootstrapServers("localhost:9092").setDataFormat("json")
			.setTopic("iris");
		sink.linkFrom(data);
		StreamOperator.execute();
	}
}
```
