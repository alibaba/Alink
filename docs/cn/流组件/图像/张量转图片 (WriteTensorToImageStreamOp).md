# 张量转图片 (WriteTensorToImageStreamOp)
Java 类名：com.alibaba.alink.operator.stream.image.WriteTensorToImageStreamOp

Python 类名：WriteTensorToImageStreamOp


## 功能介绍

将张量列转换为图片，并写入根目录对应的相对路径列中，然后原样输出结果。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| relativeFilePathCol | 文件路径列 | 文件路径列 | String | ✓ | 所选列类型为 [STRING] |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| tensorCol | tensor列 | tensor列 | String | ✓ | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] |  |
| imageType | 图片类型 | 图片类型 | String |  | "PNG", "JPEG" | "PNG" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

## 代码示例

### Python 代码

```python
df_data = pd.DataFrame([
    'sphx_glr_plot_scripted_tensor_transforms_001.png'
])

stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'path string')

readImageToTensorStreamOp = ReadImageToTensorStreamOp()\
    .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
	.setRelativeFilePathCol("path")\
	.setOutputCol("tensor")

writeTensorToImageStreamOp = WriteTensorToImageStreamOp()\
			.setRootFilePath("/tmp/write_tensor_to_image")\
			.setTensorCol("tensor")\
			.setImageType("png")\
			.setRelativeFilePathCol("path")

stream_data.link(readImageToTensorStreamOp).link(writeTensorToImageStreamOp).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.image.HasImageType.ImageType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class WriteTensorToImageStreamOpTest {

	@Test
	public void testWriteTensorToImageStreamOp() throws Exception {

		List <Row> data = Collections.singletonList(
			Row.of("sphx_glr_plot_scripted_tensor_transforms_001.png")
		);

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "path string");

		ReadImageToTensorStreamOp readImageToTensorStreamOp = new ReadImageToTensorStreamOp()
			.setRootFilePath("https://pytorch.org/vision/stable/_images/")
			.setRelativeFilePathCol("path")
			.setOutputCol("tensor");

		WriteTensorToImageStreamOp writeTensorToImageStreamOp = new WriteTensorToImageStreamOp()
			.setRootFilePath("/tmp/write_tensor_to_image")
			.setTensorCol("tensor")
			.setImageType(ImageType.PNG)
			.setRelativeFilePathCol("path");

		memSourceStreamOp.link(readImageToTensorStreamOp).link(writeTensorToImageStreamOp).print();

		StreamOperator.execute();
	}
}
```

### 运行结果

可以在 [/tmp/write_tensor_to_image/sphx_glr_plot_scripted_tensor_transforms_001.png](/tmp/write_tensor_to_image/sphx_glr_plot_scripted_tensor_transforms_001.png) 中找到 [https://pytorch.org/vision/stable/_images/sphx_glr_plot_scripted_tensor_transforms_001.png](https://pytorch.org/vision/stable/_images/sphx_glr_plot_scripted_tensor_transforms_001.png) 

同时组件的输出结果为：

| path                                             | tensor                         |
|--------------------------------------------------+--------------------------------|
| sphx_glr_plot_scripted_tensor_transforms_001.png | FLOAT#250,520,4#255.0 255.0... |
