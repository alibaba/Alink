package com.alibaba.alink.operator.batch.image;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.image.HasImageType.ImageType;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class WriteTensorToImageBatchOpTest extends AlinkTestBase {

	@Ignore
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

		List<Row> rows = memSourceBatchOp.link(readImageToTensorBatchOp).link(writeTensorToImageBatchOp).collect();
		Assert.assertEquals(1, rows.size());
	}

}