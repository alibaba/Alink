# Text文件导出 (TextSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.TextSinkStreamOp

Python 类名：TextSinkStreamOp


## 功能介绍

按行写出到文件

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

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"

data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).select("category")

sink = TextSinkStreamOp().setFilePath('/tmp/text.csv').setOverwriteSink(True)
data.link(sink)
StreamOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.TextSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import org.junit.Test;

public class TextSinkStreamOpTest {
	@Test
	public void testTextSinkStreamOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		StreamOperator <?> data = new CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).select("category");
		StreamOperator <?> sink = new TextSinkStreamOp().setFilePath("/tmp/text.csv").setOverwriteSink(true);
		data.link(sink);
		StreamOperator.execute();
	}
}
```
