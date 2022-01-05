package com.alibaba.alink.operator.stream.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class VectorToTensorStreamOpTest extends AlinkTestBase {

	@Test
	public void testVectorToTensorStreamOp() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(data, "vec string");

		CollectSinkStreamOp collectSinkStreamOp = memSourceStreamOp
			.link(
				new VectorToTensorStreamOp()
					.setSelectedCol("vec")
			)
			.link(new CollectSinkStreamOp());

		StreamOperator.execute();

		List <Row> ret = collectSinkStreamOp.getAndRemoveValues();

		Assert.assertEquals(1, ret.size());
		Assert.assertTrue(ret.get(0).getField(0) instanceof DoubleTensor);
		Assert.assertEquals(TensorUtil.getTensor("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1"), ret.get(0).getField(0));
	}
}