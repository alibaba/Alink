package com.alibaba.alink.operator.stream.image;

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