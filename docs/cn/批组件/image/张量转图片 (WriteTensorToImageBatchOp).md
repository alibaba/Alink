# 张量转图片 (WriteTensorToImageBatchOp)
Java 类名：com.alibaba.alink.operator.batch.image.WriteTensorToImageBatchOp

Python 类名：WriteTensorToImageBatchOp


## 功能介绍

将张量列转换为图片，并写入根目录对应的相对路径列中，然后原样输出结果。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| relativeFilePathCol | Not available! | Not available! | String | ✓ |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |
| tensorCol | Not available! | Not available! | String | ✓ |  |
| imageType | Not available! | Not available! | String |  | "PNG" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 代码示例

### Python 代码

```python
df_data = pd.DataFrame([
    'sphx_glr_plot_scripted_tensor_transforms_001.png'
])

batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'path string')

readImageToTensorBatchOp = ReadImageToTensorBatchOp()\
    .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
	.setRelativeFilePathCol("path")\
	.setOutputCol("tensor")

writeTensorToImageBatchOp = WriteTensorToImageBatchOp()\
			.setRootFilePath("/tmp/write_tensor_to_image")\
			.setTensorCol("tensor")\
			.setImageType("png")\
			.setRelativeFilePathCol("path")

batch_data.link(readImageToTensorBatchOp).link(writeTensorToImageBatchOp).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.image.HasImageType.ImageType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class WriteTensorToImageBatchOpTest {

	@Test
	public void testWriteTensorToImageBatchOp() throws Exception {

		List <Row> data = Collections.singletonList(
			Row.of("sphx_glr_plot_scripted_tensor_transforms_001.png")
		);

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "path string");

		ReadImageToTensorBatchOp readImageToTensorBatchOp = new ReadImageToTensorBatchOp()
			.setRootFilePath("https://pytorch.org/vision/stable/_images/")
			.setRelativeFilePathCol("path")
			.setOutputCol("tensor");

		WriteTensorToImageBatchOp writeTensorToImageBatchOp = new WriteTensorToImageBatchOp()
			.setRootFilePath("/tmp/write_tensor_to_image")
			.setTensorCol("tensor")
			.setImageType(ImageType.PNG)
			.setRelativeFilePathCol("path");

		memSourceBatchOp.link(readImageToTensorBatchOp).link(writeTensorToImageBatchOp).print();
	}

}
```

### 运行结果

可以在 [/tmp/write_tensor_to_image/sphx_glr_plot_scripted_tensor_transforms_001.png](/tmp/write_tensor_to_image/sphx_glr_plot_scripted_tensor_transforms_001.png) 中找到 [https://pytorch.org/vision/stable/_images/sphx_glr_plot_scripted_tensor_transforms_001.png](https://pytorch.org/vision/stable/_images/sphx_glr_plot_scripted_tensor_transforms_001.png) 

同时组件的输出结果为：

| path                                             | tensor                         |
|--------------------------------------------------+--------------------------------|
| sphx_glr_plot_scripted_tensor_transforms_001.png | FLOAT#250,520,4#255.0 255.0... |
