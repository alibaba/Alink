# AK文件数据源 (AkSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.AkSourceStreamOp

Python 类名：AkSourceStreamOp


## 功能介绍
以流式的方式读Ak文件。Ak文件格式是Alink 自定义的一种文件格式，能够将数据的Schema保留输出的文件格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])


batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')

filePath = "/tmp/test_alink_file_sink";

# write file to local disk
batchData.link(AkSinkBatchOp()\
				.setFilePath(FilePath(filePath))\
				.setOverwriteSink(True)\
				.setNumFiles(1))
BatchOperator.execute()

# read ak file and print
AkSourceStreamOp().setFilePath(FilePath(filePath)).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.AkSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AkSourceStreamOpTest {
	@Test
	public void testAkSourceStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int, label int");
		String filePath = "/tmp/test_alink_file_sink";
		batchData.link(new AkSinkBatchOp()
			.setFilePath(new FilePath(filePath))
			.setOverwriteSink(true)
			.setNumFiles(1));
		BatchOperator.execute();
		new AkSourceStreamOp().setFilePath(new FilePath(filePath)).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f0|f1|label
---|---|-----
4|3|2
2|1|1
2|2|1
4|3|2
2|4|1
3|2|1
1|2|1
