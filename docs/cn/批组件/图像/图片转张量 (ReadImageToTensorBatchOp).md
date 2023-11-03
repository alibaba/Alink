# 图片转张量 (ReadImageToTensorBatchOp)
Java 类名：com.alibaba.alink.operator.batch.image.ReadImageToTensorBatchOp

Python 类名：ReadImageToTensorBatchOp


## 功能介绍

将图片列转换为张量。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| relativeFilePathCol | 文件路径列 | 文件路径列 | String | ✓ | 所选列类型为 [STRING] |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| channelFirst | Channel维度是否放在Shape的第一位 | 默认为false，参数为false时，放在Shape的最右侧，为true时，放在Shape的最左侧。 | Boolean |  |  | false |
| imageHeight | 图片高度 | 图片高度 | Integer |  |  |  |
| imageWidth | 图片宽度 | 图片宽度 | Integer |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

## 代码示例

### Python 代码

```python
df_data = pd.DataFrame([
    'sphx_glr_plot_scripted_tensor_transforms_001.png'
])

batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'path string')

ReadImageToTensorBatchOp()\
    .setRootFilePath("http://alink-test-datatset.oss-cn-hangzhou-zmf.aliyuncs.com/images/")\
	.setRelativeFilePathCol("path")\
	.setOutputCol("tensor")\
    .linkFrom(batch_data)\
    .print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReadImageToTensorBatchOpTest {

	@Test
	public void testReadImageToTensorBatchOp() throws Exception {

		List <Row> data = Collections.singletonList(
			Row.of("sphx_glr_plot_scripted_tensor_transforms_001.png")
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "path string");

		new ReadImageToTensorBatchOp()
			.setRootFilePath("https://pytorch.org/vision/stable/_images/")
			.setRelativeFilePathCol("path")
			.setOutputCol("tensor")
			.linkFrom(memSourceBatchOp)
			.print();
	}

}
```

### 运行结果

| path                                             | tensor                         |
|--------------------------------------------------+--------------------------------|
| sphx_glr_plot_scripted_tensor_transforms_001.png | FLOAT#250,520,4#1.0 1.0 1.0... |
