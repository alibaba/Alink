package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class FpGrowthBatchOpTest extends AlinkTestBase {
	@Test
	public void testFpGrowth() throws Exception {
		Row[] rows = new Row[] {
			Row.of("A,B,C,D"),
			Row.of("B,C,E"),
			Row.of("A,B,C,E"),
			Row.of("B,D,E"),
			Row.of("A,B,C,D"),
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"items"});

		FpGrowthBatchOp fpGrowth = new FpGrowthBatchOp()
			.setItemsCol("items")
			.setMinSupportPercent(0.4)
			.setMinConfidence(0.6);

		fpGrowth.linkFrom(BatchOperator.fromTable(data));
		Assert.assertEquals(fpGrowth.count(), 19);
		Assert.assertEquals(fpGrowth.getSideOutput(0).count(), 27);
	}

	@Test
	public void testFpGrowth2() throws Exception {
		Row[] rows = new Row[] {
			Row.of("A,B,C,D"),
			Row.of("B,C,E"),
			Row.of("A,B,C,E"),
			Row.of("B,D,E"),
			Row.of("A,B,C,D"),
		};

		Table data = MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"items"});

		FpGrowthBatchOp fpGrowth = new FpGrowthBatchOp()
			.setItemsCol("items")
			.setMaxConsequentLength(2)
			.setMinSupportPercent(0.4)
			.setMinConfidence(0.6);

		fpGrowth.linkFrom(BatchOperator.fromTable(data));
		Assert.assertEquals(fpGrowth.getSideOutput(0).count(), 39);
	}

}