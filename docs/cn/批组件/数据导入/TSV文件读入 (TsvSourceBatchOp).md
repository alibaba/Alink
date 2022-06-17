# TSV文件读入 (TsvSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.TsvSourceBatchOp

Python 类名：TsvSourceBatchOp


## 功能介绍
读Tsv文件，Tsv文件是以tab为分隔符。文件来源可以是本地，oss，http，hdfs等。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  |  | false |
| partitions | 分区名 | 1)单级、单个分区示例：ds=20190729；2)多级分区之间用" / "分隔，例如：ds=20190729/dt=12； 3)多个分区之间用","分隔，例如：ds=20190729,ds=20190730 | String |  |  | null |
| skipBlankLine | 是否忽略空行 | 是否忽略空行 | Boolean |  |  | true |


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

filepath = "/tmp/abc.tsv"
tsvSink = TsvSinkBatchOp()\
    .setFilePath(filepath)\
    .setOverwriteSink(True)

source.link(tsvSink)

BatchOperator.execute()

tsvSource = TsvSourceBatchOp().setFilePath(filepath).setSchemaStr("f string")
tsvSource.print()

```
### Java 代码
```java
package javatest.com.alibaba.alink.batch.source;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.TsvSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TsvSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TsvSourceBatchOpTest {
	@Test
	public void testTsvSourceBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("0L", "1L", 0.6),
			Row.of("2L", "2L", 0.8),
			Row.of("2L", "4L", 0.6),
			Row.of("3L", "1L", 0.6),
			Row.of("3L", "2L", 0.3),
			Row.of("3L", "4L", 0.4)
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "uid string, iid string, label double");
		String filepath = "/tmp/abc.tsv";
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

### 运行结果
|f
|---
|0L
|2L
|3L
|3L
|2L
|3L

