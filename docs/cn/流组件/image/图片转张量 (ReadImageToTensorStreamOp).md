# 图片转张量 (ReadImageToTensorStreamOp)
Java 类名：com.alibaba.alink.operator.stream.image.ReadImageToTensorStreamOp

Python 类名：ReadImageToTensorStreamOp


## 功能介绍

将图片列转换为张量。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| relativeFilePathCol | 文件路径列 | 文件路径列 | String | ✓ |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| imageWidth | 图片宽度 | 图片宽度 | Integer |  |  |
| imageHeight | 图片高度 | 图片高度 | Integer |  |  |

## 代码示例

### Python 代码

```python
df_data = pd.DataFrame([
    'sphx_glr_plot_scripted_tensor_transforms_001.png'
])

stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'path string')

ReadImageToTensorStreamOp()\
    .setRootFilePath("https://pytorch.org/vision/stable/_images/")\
	.setRelativeFilePathCol("path")\
	.setOutputCol("tensor")\
    .linkFrom(stream_data)\
    .print()
    
StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReadImageToTensorStreamOpTest {

	@Test
	public void testReadImageToTensorStreamOp() throws Exception {

		List <Row> data = Collections.singletonList(
			Row.of("sphx_glr_plot_scripted_tensor_transforms_001.png")
		);

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "path string");

		new ReadImageToTensorStreamOp()
			.setRootFilePath("https://pytorch.org/vision/stable/_images/")
			.setRelativeFilePathCol("path")
			.setOutputCol("tensor")
			.linkFrom(memSourceStreamOp)
			.print();

		StreamOperator.execute();
	}

}
```

### 运行结果

| path                                             | tensor                         |
|--------------------------------------------------+--------------------------------|
| sphx_glr_plot_scripted_tensor_transforms_001.png | FLOAT#250,520,4#1.0 1.0 1.0... |
