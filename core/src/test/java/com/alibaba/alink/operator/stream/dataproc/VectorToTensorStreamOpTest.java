package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class VectorToTensorStreamOpTest {

	@Test
	public void testVectorToTensorStreamOp() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "vec string");

		memSourceStreamOp
			.link(
				new VectorToTensorStreamOp()
					.setSelectedCol("vec")
			)
			.print();

		StreamOperator.execute();
	}
}