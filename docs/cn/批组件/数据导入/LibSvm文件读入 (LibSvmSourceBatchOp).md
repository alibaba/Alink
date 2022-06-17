# LibSvm文件读入 (LibSvmSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp

Python 类名：LibSvmSourceBatchOp


## 功能介绍
读LibSVM文件。支持从本地、hdfs、oss、http等读取。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| partitions | 分区名 | 1)单级、单个分区示例：ds=20190729；2)多级分区之间用" / "分隔，例如：ds=20190729/dt=12； 3)多个分区之间用","分隔，例如：ds=20190729,ds=20190730 | String |  |  | null |
| startIndex | 起始索引 | 起始索引 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ['1:2.0 2:1.0 4:0.5', 1.5],
    ['1:2.0 2:1.0 4:0.5', 1.7],
    ['1:2.0 2:1.0 4:0.5', 3.6]
])
 
batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2  double')

filepath = '/tmp/abc.svm'

sink = LibSvmSinkBatchOp().setFilePath(filepath).setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
batch_data = batch_data.link(sink)

BatchOperator.execute()

batch_data = LibSvmSourceBatchOp().setFilePath(filepath)
batch_data.print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp;
import com.alibaba.alink.operator.batch.source.LibSvmSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LibSvmSourceBatchOpTest {
	@Test
	public void testLibSvmSourceBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("1:2.0 2:1.0 4:0.5", 1.5),
			Row.of("1:2.0 2:1.0 4:0.5", 1.7),
			Row.of("1:2.0 2:1.0 4:0.5", 3.6)
		);
		BatchOperator <?> batch_data = new MemSourceBatchOp(df_data, "f1 string, f2  double");
		String filepath = "/tmp/abc.svm";
		BatchOperator <?> sink = new LibSvmSinkBatchOp().setFilePath(filepath).setLabelCol("f2").setVectorCol("f1")
			.setOverwriteSink(true);
		batch_data = batch_data.link(sink);
		BatchOperator.execute();
		batch_data = new LibSvmSourceBatchOp().setFilePath(filepath);
		batch_data.print();
	}
}
```

### 运行结果

label|features
-----|--------
1.7000|1:2.0 2:1.0 4:0.5
1.5000|1:2.0 2:1.0 4:0.5
3.6000|1:2.0 2:1.0 4:0.5
