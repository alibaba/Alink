package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class TensorToVectorStreamOpTest extends AlinkTestBase {

	@Test
	public void testTensorToVectorStreamOp() throws Exception {

		List <Row> data = Collections.singletonList(Row.of("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1"));

		CollectSinkStreamOp collectSinkStreamOp =
			new MemSourceStreamOp(data, "tensor string")
				.link(
					new TensorToVectorStreamOp()
						.setSelectedCol("tensor")
				)
				.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		List <Row> ret = collectSinkStreamOp.getAndRemoveValues();

		Assert.assertEquals(1, ret.size());
		Assert.assertTrue(ret.get(0).getField(0) instanceof DenseVector);
		Assert.assertArrayEquals(
			new double[] {0.0, 0.1, 1.0, 1.1, 2.0, 2.1},
			((DenseVector) ret.get(0).getField(0)).getData(),
			1e-6
		);
	}
}