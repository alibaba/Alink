package com.alibaba.alink.operator.stream.image;

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