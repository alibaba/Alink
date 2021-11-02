# LibSvm文件数据源 (LibSvmSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.LibSvmSourceStreamOp

Python 类名：LibSvmSourceStreamOp


## 功能介绍
读LibSVM文件。支持从本地、hdfs读取。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| startIndex | 起始索引 | 起始索引 | Integer |  | 1 |

## 代码示例

### Python 代码

```python
df_data = pd.DataFrame([
    ['1:2.0 2:1.0 4:0.5', 1.5],
    ['1:2.0 2:1.0 4:0.5', 1.7],
    ['1:2.0 2:1.0 4:0.5', 3.6]
])
 
stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 string, f2  double')

filepath = '/tmp/abc.svm'

sink = LibSvmSinkStreamOp().setFilePath(filepath).setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
stream_data = stream_data.link(sink)

StreamOperator.execute()

stream_data = LibSvmSourceStreamOp().setFilePath(filepath)
stream_data.print()

StreamOperator.execute()

```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.LibSvmSinkStreamOp;
import com.alibaba.alink.operator.stream.source.LibSvmSourceStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LibSvmSourceStreamOpTest {

	@Test
	public void testLibSvmSourceStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("1:2.0 2:1.0 4:0.5", 1.5),
			Row.of("1:2.0 2:1.0 4:0.5", 1.7),
			Row.of("1:2.0 2:1.0 4:0.5", 3.6)
		);
		StreamOperator <?> streamData = new MemSourceStreamOp(df_data, "f1 string, f2  double");
		String filepath = "/tmp/abc.svm";

		StreamOperator <?> sink = new LibSvmSinkStreamOp()
			.setFilePath(filepath)
			.setLabelCol("f2")
			.setVectorCol("f1")
			.setOverwriteSink(true);

		streamData.link(sink);

		StreamOperator.execute();

		StreamOperator <?> stream_data = new LibSvmSourceStreamOp().setFilePath(filepath);
		stream_data.print();

		StreamOperator.execute();
	}
}
```

### 运行结果
label|features
-----|--------
1.7000|1:2.0 2:1.0 4:0.5
3.6000|1:2.0 2:1.0 4:0.5
1.5000|1:2.0 2:1.0 4:0.5
