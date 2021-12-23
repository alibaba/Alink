package com.alibaba.alink.operator.batch.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TensorToVectorBatchOpTest {

	@Test
	public void testTensorToVectorBatchOp() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "tensor string");

		memSourceBatchOp
			.link(
				new TensorToVectorBatchOp()
					.setSelectedCol("tensor")
			)
			.print();
	}
}