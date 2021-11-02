# LibSvm文件导出 (LibSvmSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp

Python 类名：LibSvmSinkBatchOp


## 功能介绍

写出LibSvm格式文件，支持写出到本地文件和HDFS文件。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  | false |
| startIndex | 起始索引 | 起始索引 | Integer |  | 1 |



## 代码示例

### Python 代码
```python
df_data = pd.DataFrame([
    ['1:2.0 2:1.0 4:0.5', 1.5],
    ['1:2.0 2:1.0 4:0.5', 1.7],
    ['1:2.0 2:1.0 4:0.5', 3.6]
])
 
batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2  double')

sink = LibSvmSinkBatchOp().setFilePath('/tmp/abc.svm').setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
batch_data = batch_data.link(sink)

BatchOperator.execute()

```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.LibSvmSinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LibSvmSinkBatchOpTest {
	@Test
	public void testLibSvmSinkBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("1:2.0 2:1.0 4:0.5", 1.5),
			Row.of("1:2.0 2:1.0 4:0.5", 1.7),
			Row.of("1:2.0 2:1.0 4:0.5", 3.6)
		);
		BatchOperator <?> batch_data = new MemSourceBatchOp(df_data, "f1 string, f2  double");
		BatchOperator <?> sink = new LibSvmSinkBatchOp().setFilePath("/tmp/abc.svm").setLabelCol("f2").setVectorCol(
			"f1").setOverwriteSink(true);
		batch_data.link(sink);
		BatchOperator.execute();
	}
}
```
