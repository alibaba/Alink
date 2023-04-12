# TFRecord Dataset文件导出 (TFRecordDatasetSinkBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sink.TFRecordDatasetSinkBatchOp

Python 类名：TFRecordDatasetSinkBatchOp


## 功能介绍

写出 TFRecordDataset 文件（TFRecordDataset 的介绍可以参考 TensorFlow 文档：https://www.tensorflow.org/tutorials/load_data/tfrecord ）。

### 使用说明

需要指定文件路径 filePath，默认为单并行度写出单个文件。
如果希望并行的写出文件，那么需要设置参数numFiles，得到的是一个包含多个 TFRecordDataset 文件的目录。

TFRecord 中， Feature 允许的数据类型仅有 float, int64, bytes。
其中数据类型为 bytes时，存储的数据实际上为 ByteString 的列表；为 float, int64 时存储的数据为 float 或 int64 的列表。
数据写出时会根据在 Alink 中的类型进行转换， 其他类型请先使用类型转换组件进行转换：

- DOUBLE, FLOAT, BIG_DEC：转为float特征；
- LONG, INT, BIG_INT, SHORT：转为int64特征；
- STRING：转为bytes特征，按 UTF8 编码对应 1 个ByteString；
- DENSE_VECTOR：转为float特征；
- FLOAT_TENSOR, DOUBLE_TENSOR：转为float特征，数据被展平为1维；
- INT_TENSOR, LONG_TENSOR：转为int64特征，数据被展平为1维；
- BYTE_TENSOR：转为bytes特征，rank = 1 时对应 1 个ByteString，rank = 2时对应ByteString的列表，其他 rank 不支持；
- STRING_TENSOR：转为bytes特征，按 UTF8 编码对应ByteString的列表；
- VARBINARY：转为bytes特征，对应 1 个ByteString。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| numFiles | 文件数目 | 文件数目 | Integer |  |  | 1 |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |

## 代码示例

### Python 代码

```python
schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
source = CsvSourceBatchOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv") \
    .setSchemaStr(schemaStr)
sink = TFRecordDatasetSinkBatchOp() \
    .setFilePath("/tmp/iris.tfrecord") \
    .setOverwriteSink(True) \
    .linkFrom(source)
BatchOperator.execute()
```

### Java 代码

```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.TFRecordDatasetSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import org.junit.Test;

public class TFRecordDatasetSinkBatchOpTest {
	@Test
	public void testTFRecordDatasetSinkBatchOp() throws Exception {
		String schemaStr
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> source = new CsvSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
			.setSchemaStr(schemaStr);
		BatchOperator <?> sink = new TFRecordDatasetSinkBatchOp()
			.setFilePath("/tmp/iris.tfrecord")
			.setOverwriteSink(true)
			.linkFrom(source);
		BatchOperator.execute();
	}
}
```
