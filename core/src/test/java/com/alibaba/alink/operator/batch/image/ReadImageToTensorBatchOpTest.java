package com.alibaba.alink.operator.batch.image;

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