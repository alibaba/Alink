# TSV文件导出 (TsvSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.TsvSinkStreamOp

Python 类名：TsvSinkStreamOp


## 功能介绍

写Tsv文件，Tsv文件是以tab为分隔符

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
                ["0L", "1L", 0.6],
                ["2L", "2L", 0.8],
                ["2L", "4L", 0.6],
                ["3L", "1L", 0.6],
                ["3L", "2L", 0.3],
                ["3L", "4L", 0.4]
        ])

source = StreamOperator.fromDataframe(df, schemaStr='uid string, iid string, label double')

filepath = "/tmp/abc.tsv"
tsvSink = TsvSinkStreamOp()\
    .setFilePath(filepath)\
    .setOverwriteSink(True)

source.link(tsvSink)

StreamOperator.execute()

tsvSource = TsvSourceStreamOp().setFilePath(filepath).setSchemaStr("f string");
tsvSource.print()

StreamOperator.execute()


```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.TsvSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TsvSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TsvSinkStreamOpTest {
	@Test
	public void testTsvSinkStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0L", "1L", 0.6),
			Row.of("2L", "2L", 0.8),
			Row.of("2L", "4L", 0.6),
			Row.of("3L", "1L", 0.6),
			Row.of("3L", "2L", 0.3),
			Row.of("3L", "4L", 0.4)
		);
		StreamOperator <?> source = new MemSourceStreamOp(df, "uid string, iid string, label double");
		String filepath = "/tmp/abc.tsv";
		StreamOperator <?> tsvSink = new TsvSinkStreamOp()
			.setFilePath(filepath)
			.setOverwriteSink(true);
		source.link(tsvSink);
		StreamOperator.execute();
		StreamOperator <?> tsvSource = new TsvSourceStreamOp().setFilePath(filepath).setSchemaStr("f string");
		tsvSource.print();
		StreamOperator.execute();
	}
}
```
