package com.alibaba.alink.operator.batch;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.sink.DummySinkBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BatchOperatorTest extends AlinkTestBase {
	private static final Row[] trainArrayData = new Row[] {
		Row.of(1L, 1L, 0.6),
		Row.of(2L, 2L, 0.8),
		Row.of(2L, 3L, 0.6),
		Row.of(3L, 1L, 0.6),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.4),
	};

	@Test
	public void testCollect() throws Exception {
		MemSourceBatchOp memSourceBatchOp
			= new MemSourceBatchOp(Arrays.asList(trainArrayData), new String[] {"u", "i", "r"});

		List <List <Row>> ret = BatchOperator.collect(memSourceBatchOp);

		System.out.println(JsonConverter.toJson(ret));
	}

	@Test
	public void testSinkWithoutCollectInNonDefaultEnv() throws Exception {
		Long mlEnvId = MLEnvironmentFactory.getNewMLEnvironmentId();
		MemSourceBatchOp memSourceBatchOp
			= new MemSourceBatchOp(Arrays.asList(trainArrayData), new String[] {"u", "i", "r"})
			.setMLEnvironmentId(mlEnvId);
		memSourceBatchOp.link(new DummySinkBatchOp().setMLEnvironmentId(mlEnvId));
		BatchOperator.execute(MLEnvironmentFactory.get(mlEnvId));
	}
}
