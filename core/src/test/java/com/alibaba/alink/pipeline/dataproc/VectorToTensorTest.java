package com.alibaba.alink.pipeline.dataproc;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.tensor.TensorUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class VectorToTensorTest extends AlinkTestBase {

	@Test
	public void testVectorToTensor() throws Exception {
		List <Row> data = Collections.singletonList(Row.of("0.0 0.1 1.0 1.1 2.0 2.1"));

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(data, "vec string");

		new VectorToTensor()
			.setSelectedCol("vec")
			.transform(memSourceBatchOp)
			.lazyCollect(rows -> Assert.assertEquals(
				Row.of(TensorUtil.getTensor("DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1")),
				rows.get(0)
			));

		BatchOperator.execute();
	}

}