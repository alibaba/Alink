# TF SavedModel模型预测 (TFSavedModelPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.tensorflow.TFSavedModelPredictStreamOp

Python 类名：TFSavedModelPredictStreamOp


## 功能介绍

该组件支持直接使用 SavedModel 进行预测。

模型路径需要时一个压缩文件，解压后能得到一个目录，目录内包含 SavedModel 的文件。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| modelPath | 模型的URL路径 | 模型的URL路径 | String | ✓ |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |
| graphDefTag | graph标签 | graph标签 | String |  | "serve" |
| inputSignatureDefs | 输入 SignatureDef | SavedModel 模型的输入 SignatureDef 名，用逗号分隔，需要与输入列一一对应，默认与选择列相同 | String[] |  | null |
| outputSignatureDefs | TF 输出 SignatureDef 名 | 模型的输出 SignatureDef 名，多个输出时用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同 | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  | null |
| signatureDefKey | signature标签 | signature标签 | String |  | "serving_default" |


### 模型路径说明

模型路径可以是以下形式：
- 本地文件：```file://``` 加绝对路径，例如 ```file:///tmp/dnn.py```；
- Java 包中的资源文件：```res://``` 加路径，例如 ```res:///dnn.py```；
- http/https 文件：```http://``` 或 ```https://``` 路径；
- OSS 文件：```oss://``` 加路径和 Endpoint 和 access key 等信息，例如```oss://bucket/xxx/xxx/xxx.py?host=xxx&access_key_id=xxx&access_key_secret=xxx```；
- HDFS 文件：```hdfs://``` 加路径；

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
url = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_dense.csv"
schema = "label bigint, image string";

data = CsvSourceStreamOp() \
    .setFilePath(url) \
    .setSchemaStr(schema) \
    .setFieldDelimiter(";")

predictor = TFSavedModelPredictStreamOp() \
    .setModelPath("http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip") \
    .setSelectedCols(["image"]) \
    .setOutputSchemaStr("classes bigint, probabilities string")

data = predictor.linkFrom(data).select("label, classes, probabilities")
data.print()
StreamOperator.execute()
```

### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.tensorflow.TFSavedModelPredictStreamOp;
import org.junit.Test;

public class TFSavedModelPredictStreamOpTest {

	@Test
	public void testTFSavedModelPredictStreamOp() throws Exception {
		String url = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/mnist_dense.csv";
		String schema = "label bigint, image string";

		StreamOperator <?> data = new CsvSourceStreamOp().setFilePath(url).setSchemaStr(schema).setFieldDelimiter(";");

		StreamOperator <?> predictor = new TFSavedModelPredictStreamOp()
			.setModelPath("http://alink-dataset.oss-cn-zhangjiakou.aliyuncs.com/tf/1551968314.zip")
			.setSelectedCols("image")
			.setOutputSchemaStr("classes bigint, probabilities string");

		data = predictor.linkFrom(data).select("label, classes, probabilities");
		data.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
label|classes|probabilities
-----|-------|-------------
6|6|0.0 0.0 0.0 0.0 0.0 0.0 1.0 0.0 0.0 0.0
0|0|1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0
3|3|0.0 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0
6|6|0.0 0.0 0.0 0.0 0.0 0.0 1.0 0.0 0.0 0.0
5|5|0.0 0.0 0.0 0.0 0.0 1.0 0.0 0.0 0.0 0.0
...|...|...
