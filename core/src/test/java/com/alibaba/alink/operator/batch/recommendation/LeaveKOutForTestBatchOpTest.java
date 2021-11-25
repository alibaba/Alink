package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for LooSplit.
 */

public class LeaveKOutForTestBatchOpTest extends AlinkTestBase {
	private final Row[] rows1 = new Row[] {
		Row.of(1L, 1L, 0.7),
		Row.of(1L, 2L, 0.1),
		Row.of(1L, 3L, 0.6),
		Row.of(1L, 1L, 0.5),
		Row.of(2L, 2L, 0.8),
		Row.of(2L, 3L, 0.6),
		Row.of(2L, 1L, 0.0),
		Row.of(2L, 2L, 0.7),
		Row.of(2L, 3L, 0.4),
		Row.of(3L, 1L, 0.6),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.4),
		Row.of(3L, 1L, 0.9),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.1),
	};

	@Test
	public void leaveKOutsplit() throws Exception {
		BatchOperator <?> data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[] {"user", "item", "rating"}));

		BatchOperator <?> spliter = new LeaveKObjectOutBatchOp()
			.setK(2)
			.setGroupCol("user")
			.setObjectCol("item")
			.setOutputCol("label");
		BatchOperator <?> left = spliter.linkFrom(data);
		BatchOperator <?> right = spliter.getSideOutput(0);

		Assert.assertEquals(right.count(), 9);
		Assert.assertEquals(left.count(), 3);
	}

	@Test
	public void leaveTopKOutsplit() throws Exception {
		BatchOperator <?> data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[] {"user", "item", "rating"}));

		BatchOperator <?> spliter = new LeaveTopKObjectOutBatchOp()
			.setK(2)
			.setRateThreshold(0.7)
			.setRateCol("rating")
			.setObjectCol("item")
			.setOutputCol("label")
			.setGroupCol("user");
		BatchOperator <?> left = spliter.linkFrom(data);
		BatchOperator <?> right = spliter.getSideOutput(0);
		Assert.assertEquals(right.count(), 11);
		Assert.assertEquals(left.count(), 3);
	}
}