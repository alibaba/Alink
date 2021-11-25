package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TensorToVectorTest {

	@Test
	public void testTensorToVector() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "tensor string");

		new TensorToVector()
			.setSelectedCol("tensor")
			.transform(memSourceBatchOp)
			.print();
	}
}