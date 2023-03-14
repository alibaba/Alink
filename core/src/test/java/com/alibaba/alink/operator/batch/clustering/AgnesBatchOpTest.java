package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

public class AgnesBatchOpTest extends AlinkTestBase {
	private List <Row> inputRows;

	@Before
	public void init() {
		inputRows = Arrays.asList(
			Row.of(0, 0, "id_1", "2.0, 3.0"),
			Row.of(0, 0, "id_2", "2.1, 3.1"),
			Row.of(0, 0, "id_3", "200.1, 300.1"),
			Row.of(0, 0, "id_4", "200.2, 300.2"),
			Row.of(1, 1, "id_5", "200.3, 300.3"),
			Row.of(1, 1, "id_6", "200.4, 300.4"),
			Row.of(1, 1, "id_7", "200.5, 300.5"),
			Row.of(0, 0, "id_8", "200.6, 300.6"),
			Row.of(1, 1, "id_9", "2.1, 3.1"),
			Row.of(1, 1, "id_10", "2.1, 3.1"),
			Row.of(1, 1, "id_11", "2.1, 3.1"),
			Row.of(0, 0, "id_12", "2.1, 3.1"),
			Row.of(1, 1, "id_13", "2.3, 3.2"),
			Row.of(1, 1, "id_14", "2.3, 3.2"),
			Row.of(0, 0, "id_15", "2.8, 3.2"),
			Row.of(1, 1, "id_16", "300., 3.2"),
			Row.of(1, 1, "id_17", "2.2, 3.2"),
			Row.of(0, 0, "id_18", "2.4, 3.2"),
			Row.of(1, 1, "id_19", "2.5, 3.2"),
			Row.of(1, 1, "id_20", "2.5, 3.2"),
			Row.of(2, 1, "id_21", "2.5, 3.2"),
			Row.of(1, 1, "id_22", "2.1, 3.1")
		);
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testBatchOp() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(inputRows,
			new String[] {"group1", "group2", "id", "vec"});

		AgnesBatchOp op = new AgnesBatchOp()
			.setIdCol("id")
			.setVectorCol("vec")
			.setPredictionCol("pred")
			.linkFrom(data);
		Assert.assertEquals(op.select("pred").distinct().count(), 2);
	}

	@Test
	public void testLazy() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(inputRows,
			new String[] {"group1", "group2", "id", "vec"});
		AgnesBatchOp op = new AgnesBatchOp()
			.setIdCol("id")
			.setVectorCol("vec")
			.setPredictionCol("pred")
			.linkFrom(data);

		op.lazyPrintModelInfo();
		op.getSideOutput(0).lazyPrint(-1);
		BatchOperator.execute();
	}

	@Test
	public void testException() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(inputRows,
			new String[] {"group1", "group2", "id", "vec"});

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("k should larger than 1,or distanceThreshold should be set");
		AgnesBatchOp op = new AgnesBatchOp()
			.setIdCol("id")
			.setVectorCol("vec")
			.setPredictionCol("pred")
			.setK(1)
			.linkFrom(data);
		op.print();
	}
}