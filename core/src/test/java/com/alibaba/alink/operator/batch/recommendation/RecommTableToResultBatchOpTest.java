package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.recommendation.Zipped2KObjectBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for RecommTableToKv.
 */
public class RecommTableToResultBatchOpTest extends AlinkTestBase {
	private Row[] rows1 = new Row[] {
		Row.of(1L, 1L, 0.6),
		Row.of(2L, 2L, 0.8),
		Row.of(2L, 3L, 0.6),
		Row.of(3L, 1L, 0.6),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.4),
	};

	@Test
	public void test() throws Exception {
		BatchOperator data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[] {"user", "item", "rating"}));

		Zipped2KObjectBatchOp op = new Zipped2KObjectBatchOp()
			.setGroupCol("user")
			.setObjectCol("item")
			.setInfoCols("rating")
			.setOutputCol("recomm")
			.linkFrom(data)
			.lazyPrint(-1);

		FlattenKObjectBatchOp op1 = new FlattenKObjectBatchOp()
			.setSelectedCol("recomm")
			.setOutputColTypes("long","double")
			.setReservedCols("user")
			.setOutputCols("object", "rating")
			.linkFrom(op)
			.lazyPrint(-1);

		BatchOperator.execute();

		Assert.assertEquals(op.count(), 3);
		Assert.assertEquals(op1.count(), 6);
	}
}