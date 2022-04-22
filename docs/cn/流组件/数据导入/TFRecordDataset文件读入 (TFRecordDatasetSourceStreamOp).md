# TFRecordDataset文件读入 (TFRecordDatasetSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.TFRecordDatasetSourceStreamOp

Python 类名：TFRecordDatasetSourceStreamOp


## 功能介绍

读取 TFRecordDataset 文件（TFRecordDataset 的介绍可以参考 TensorFlow 文档：https://www.tensorflow.org/tutorials/load_data/tfrecord ）。

### 使用说明

需要指定文件路径 ```filePath```，可以是单个文件，也可以是包含多个 TFRecordDataset 的目录。

为了将读取的 ```TFRecord``` 转换为 Alink 中的格式，需要指定数据的 ```schemaStr```。
由于 ```TFRecord``` 中 ```Feature``` 允许的数据类型仅有 ```float, int64, bytes```，因此 ```schemaStr``` 填写的类型有一定的限制：

- ```FLOAT_TENSOR```/```DOUBLE_TENSOR```：要求```float```特征；
- ```LONG_TENSOR```/```INT_TENSOR```：要求```int64```特征；
- ```STRING_TENSOR```：要求```bytes```特征；
- ```BYTE_TENSOR```：要求```bytes```特征；
- ```DENSE_VECTOR```：要求```float```特征；
- ```LONG```/```INT```：要求```int64```特征，并且只使用第一个元素；
- ```FLOAT```/```DOUBLE```：要求```float```特征，并且只使用第一个元素；
- ```STRING```：要求```bytes```特征，并且只使用第一个元素；
- ```VARBINARY```：要求```bytes```特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |


## 代码示例

### Python 代码
```python
schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
source = TFRecordDatasetSourceStreamOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.tfrecord") \
    .setSchemaStr(schemaStr)
source.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TFRecordDatasetSourceStreamOp;
import org.junit.Test;

public class TFRecordDatasetSourceStreamOpTest {
	@Test
	public void testTFRecordDatasetSourceStreamOp() throws Exception {
		String schemaStr
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		StreamOperator <?> source = new TFRecordDatasetSourceStreamOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.tfrecord")
			.setSchemaStr(schemaStr);
		source.print();
		StreamOperator.execute();
	}
}
```
