# AK文件导出 (AkSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.AkSinkStreamOp

Python 类名：AkSinkStreamOp


## 功能介绍
将一个流式数据，以Ak文件格式写出到文件系统。Ak文件格式是Alink 自定义的一种文件格式，能够将数据的Schema保留输出的文件格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| numFiles | 文件数目 | 文件数目 | Integer |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])


streamData = StreamOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')

filePath = "/tmp/test_alink_file_sink";

# write file to local disk
streamData.link(AkSinkStreamOp()\
				.setFilePath(FilePath(filePath))\
				.setOverwriteSink(True)\
				.setNumFiles(1))
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.HadoopFileSystem;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.AkSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AkSinkStreamOpTest {
	@Test
	public void testAkSinkStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1)
		);
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "f0 int, f1 int, label int");
		String filePath = "/tmp/test_alink_file_sink";
		streamData.link(new AkSinkStreamOp()
			.setFilePath(new FilePath(filePath))
			.setOverwriteSink(true)
			.setNumFiles(1));
		String hdfsFilePath = "alink_fs_test/test_alink_file_sink";
		HadoopFileSystem fs = new HadoopFileSystem("2.8.3", "hdfs://10.101.201.169:9000");
		streamData.link(new AkSinkStreamOp()
			.setFilePath(new FilePath(hdfsFilePath, fs))
			.setOverwriteSink(true)
			.setNumFiles(1));
		StreamOperator.execute();
	}
}
```
