package com.alibaba.alink.operator.stream.image;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReadImageToTensorStreamOpTest extends AlinkTestBase {

	@Test
	public void testReadImageToTensorStreamOp() throws Exception {

		List <Row> data = Collections.singletonList(
			Row.of("sphx_glr_plot_scripted_tensor_transforms_001.png")
		);

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "path string");

		CollectSinkStreamOp collectSinkStreamOp = new ReadImageToTensorStreamOp()
			.setRootFilePath("http://alink-test-datatset.oss-cn-hangzhou-zmf.aliyuncs.com/images/")
			.setRelativeFilePathCol("path")
			.setOutputCol("tensor")
			.linkFrom(memSourceStreamOp)
			.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		List <Row> rows = collectSinkStreamOp.getAndRemoveValues();
		Assert.assertEquals(1, rows.size());
	}

}