package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class VectorToTensorTest {

	@Test
	public void testVectorToTensor() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "vec string");

		new VectorToTensor()
			.setSelectedCol("vec")
			.transform(memSourceBatchOp)
			.print();
	}

}