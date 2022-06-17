# TorchScript 模型预测 (TorchModelPredictor)
Java 类名：com.alibaba.alink.pipeline.pytorch.TorchModelPredictor

Python 类名：TorchModelPredictor


## 功能介绍

加载 TorchScript 模型进行预测。

### 使用方式

模型路径```modelPath```需要是一个通过 ```torch.jit``` 导出的模型文件路径。

参与模型预测的数据通过参数 ```selectedCols``` 设置，需要注意以下几点：

- TorchScript 模型调用 ```forward``` 方法时是通过位置来传入参数的，所以 ```selectedCols``` 中各列的顺序是有意义的。
- 所选择的列的类型需要是 Alink ```Tensor``` 类型或者 4 种基本数据类型（```Long, Double, Boolean, String``` 及其兼容类型），不接受其他类型。

模型输出信息通过参数 ```outputSchemaStr``` 指定，包括输出列名以及名称，需要注意以下几点：

- 输出列的数量需要与模型输出结果匹配。
- 输出类型可以是 Alink ```Tensor``` 类型或者 Alink 支持的类型，如果从模型预测输出的结果转换到指定类型失败那么将报错；暂不支持列表或字典类型。

组件使用的是 PyTorch 1.8.1 CPU 版本，如果需要使用 GPU 功能，可以自行替换插件文件。

在 Windows 下运行时，如果遇到 ```UnsatisfiedLinkError```，请下载 [Visual C++ 2015 Redistributable Packages](https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads) 并重启，然后重新运行。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| modelPath | 模型的URL路径 | 模型的URL路径 | String | ✓ |  |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| intraOpParallelism | Op 间并发度 | Op 间并发度 | Integer |  |  | 4 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

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
test = AkSourceBatchOp()\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_test_vector.ak");

test = VectorToTensorBatchOp()\
    .setTensorDataType("float")\
    .setTensorShape([1, 1, 28, 28])\
    .setSelectedCol("vec")\
    .setOutputCol("tensor")\
    .setReservedCols(["label"])\
    .linkFrom(test)

predictor = TorchModelPredictor()\
    .setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_pytorch.pt")\
    .setSelectedCols(["tensor"])\
    .setOutputSchemaStr("probabilities FLOAT_TENSOR")

test = predictor.transform(test).select("label, probabilities")
test.print()
```

### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.VectorToTensorBatchOp;
import com.alibaba.alink.operator.batch.source.AkSourceBatchOp;
import com.alibaba.alink.pipeline.pytorch.TorchModelPredictor;
import org.junit.Test;

public class TorchModelPredictorTest {

	@Test
	public void testTorchModelPredictor() throws Exception {
		BatchOperator.setParallelism(1);
		BatchOperator <?> test = new AkSourceBatchOp()
			.setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_test_vector.ak");

		test = new VectorToTensorBatchOp()
			.setTensorDataType("float")
			.setTensorShape(1, 1, 28, 28)
			.setSelectedCol("vec")
			.setOutputCol("tensor")
			.setReservedCols("label")
			.linkFrom(test);

		TorchModelPredictor predictor = new TorchModelPredictor()
			.setModelPath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_model_pytorch.pt")
			.setSelectedCols("tensor")
			.setOutputSchemaStr("probabilities FLOAT_TENSOR");

		test = predictor.transform(test).select("label, probabilities");
		test.print();
	}
}
```
