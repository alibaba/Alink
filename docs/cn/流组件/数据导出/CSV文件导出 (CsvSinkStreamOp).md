# CSV文件导出 (CsvSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp

Python 类名：CsvSinkStreamOp


## 功能介绍
写CSV文件

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  |  | "," |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| quoteChar | 引号字符 | 引号字符 | Character |  |  | "\"" |
| rowDelimiter | 行分隔符 | 行分隔符 | String |  |  | "\n" |

## 代码示例

### Python 代码

```python
filePath = 'https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceStreamOp()\
    .setFilePath(filePath)\
    .setSchemaStr(schema)\
    .setFieldDelimiter(",")
csvSink = CsvSinkStreamOp()\
    .setFilePath('~/csv_test_s.txt')

csvSource.link(csvSink)

StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CsvSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class CsvSinkStreamOpTest {
	@Test
	public void testCsvSinkStreamOp() throws Exception {
		String filePath = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String schema
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		CsvSourceStreamOp csvSource = new CsvSourceStreamOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setFieldDelimiter(",");
		CsvSinkStreamOp csvSink = new CsvSinkStreamOp()
			.setFilePath("~/csv_test_s.txt");

		csvSource.link(csvSink);

		StreamOperator.execute();
	}
}

```
