# TSV文件导出 (TsvSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.TsvSinkBatchOp

Python 类名：TsvSinkBatchOp


## 功能介绍

写Tsv文件，Tsv文件是以tab为分隔符

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| numFiles | 文件数目 | 文件数目 | Integer |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |


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

source = BatchOperator.fromDataframe(df, schemaStr='uid string, iid string, label double')

filepath = '*'
tsvSink = TsvSinkBatchOp()\
    .setFilePath(filepath)

source.link(tsvSink)

BatchOperator.execute()

tsvSource = TsvSourceBatchOp().setFilePath(filepath).setSchemaStr("f string");
tsvSource.print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.TsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TsvSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TsvSinkBatchOpTest {
	@Test
	public void testTsvSinkBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0L", "1L", 0.6),
			Row.of("2L", "2L", 0.8),
			Row.of("2L", "4L", 0.6),
			Row.of("3L", "1L", 0.6),
			Row.of("3L", "2L", 0.3),
			Row.of("3L", "4L", 0.4)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "uid string, iid string, label double");
		String filepath = "*";
		BatchOperator <?> tsvSink = new TsvSinkBatchOp()
			.setFilePath(filepath)
			.setOverwriteSink(true);
		source.link(tsvSink);
		BatchOperator.execute();
		BatchOperator <?> tsvSource = new TsvSourceBatchOp().setFilePath(filepath).setSchemaStr("f string");
		tsvSource.print();
	}
}
```
