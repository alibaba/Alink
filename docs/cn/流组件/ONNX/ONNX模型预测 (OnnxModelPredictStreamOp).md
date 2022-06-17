# ONNX模型预测 (OnnxModelPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.onnx.OnnxModelPredictStreamOp

Python 类名：OnnxModelPredictStreamOp


## 功能介绍

加载 ONNX 模型进行预测。

### 使用方式

模型路径```modelPath```需要是 ONNX 模型。

参与模型预测的数据通过参数 ```selectedCols``` 设置，需要注意以下几点：

- ONNX 模型使用 input name 来标识模型输入桩的，因此需要设置 ```inputNames```，与 ```selectedCols``` 一一对应，表明某列对应某输入桩。```inputNames``` 不填写时，默认与列名一致。
- 仅支持输入桩为 ```Tensor``` 类型，不支持 ```Sequences``` 和 ```Maps``` 类型。
- 所选择的列的类型需要是```float, double, int, long, byte, string``` 类型及其对应的 Alink ```Tensor``` 类型。

模型输出信息通过参数 ```outputSchemaStr``` 指定，包括输出列名以及名称，需要注意以下几点：

- ONNX 模型使用 output name 来标识模型输出桩的，因此需要设置 ```outputNames```，与 ```outputSchemaStr``` 一一对应，表明某列对应某输入桩。```outputNames``` 不填写时，默认与列名一致。
- 仅支持输出桩为 ```Tensor``` 类型，不支持 ```Sequences``` 和 ```Maps``` 类型。
- ```outputSchemaStr``` 填写的输出类型需要是对应的输出桩类型，例如 输出桩类型 为 Float 类型的 Tensor 时，对应的 Alink 类型可以是 ```TENSOR``` 或者 ```FLOAT_TENSOR```，当输出仅包含一个元素时，还可以是 ```FLOAT```。

组件使用的是 ONNX 1.11.0 版本，当有 GPU 时，自动使用 GPU 进行推理，否则使用 CPU 进行推理。

在 Windows 下运行时，如果遇到 ```UnsatisfiedLinkError```，请下载 [Visual C++ 2019 Redistributable Packages](https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads) 并重启，然后重新运行。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| modelPath | 模型的URL路径 | 模型的URL路径 | String | ✓ |  |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| inputNames | ONNX 模型输入名 | ONNX 模型输入名，用逗号分隔，需要与输入列一一对应，默认与选择列相同 | String[] |  |  | null |
| outputNames | ONNX 模型输出名 | ONNX 模型输出名，用逗号分隔，并且与输出 Schema 一一对应，默认与输出 Schema 中的列名相同 | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |

### 模型路径说明

模型路径可以是以下形式：

- 本地文件：```file://``` 加绝对路径，例如 ```file:///tmp/dnn.py```；
- Java 包中的资源文件：```res://``` 加路径，例如 ```res:///dnn.py```；
- http/https 文件：```http://``` 或 ```https://``` 路径；
- OSS 文件：```oss://``` 加路径和 Endpoint 和 access key
  等信息，例如```oss://bucket/xxx/xxx/xxx.py?host=xxx&access_key_id=xxx&access_key_secret=xxx```；
- HDFS 文件：```hdfs://``` 加路径；

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码

```python
test = AkSourceStreamOp()\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_test_vector.ak");

test = VectorToTensorStreamOp()\
    .setTensorDataType("float")\
    .setTensorShape([1, 1, 28, 28])\
    .setSelectedCol("vec")\
    .setOutputCol("tensor")\
    .setReservedCols(["label"])\
    .linkFrom(test)

predictor = OnnxModelPredictStreamOp() \
    .setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/cnn_mnist_pytorch.onnx") \
    .setSelectedCols(["tensor"]) \
    .setInputNames(["0"]) \
    .setOutputNames(["21"]) \
    .setOutputSchemaStr("probabilities FLOAT_TENSOR")

test = predictor.linkFrom(test).select("label, probabilities")
test.print()
StreamOperator.execute()
```

### Java 代码

```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.VectorToTensorStreamOp;
import com.alibaba.alink.operator.stream.onnx.OnnxModelPredictStreamOp;
import com.alibaba.alink.operator.stream.source.AkSourceStreamOp;
import org.junit.Test;

public class OnnxModelPredictStreamOpTest {
	@Test
	public void testOnnxModelPredictStreamOp() throws Exception {
		StreamOperator.setParallelism(1);
		StreamOperator <?> test = new AkSourceStreamOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_test_vector.ak");

		test = new VectorToTensorStreamOp()
			.setTensorDataType("float")
			.setTensorShape(1, 1, 28, 28)
			.setSelectedCol("vec")
			.setOutputCol("tensor")
			.setReservedCols("label")
			.linkFrom(test);

		StreamOperator <?> predictor = new OnnxModelPredictStreamOp()
			.setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/cnn_mnist_pytorch.onnx")
			.setSelectedCols("tensor")
			.setInputNames("0")
			.setOutputNames("21")
			.setOutputSchemaStr("probabilities FLOAT_TENSOR");

		test = predictor.linkFrom(test).select("label, probabilities");
		test.print();
		StreamOperator.execute();
	}
}
```
