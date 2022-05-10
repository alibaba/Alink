package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
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

	@Test
	public void testFpGrowth3() throws Exception {
		Row[] rows = new Row[] {
			Row.of(1, "A"),
			Row.of(1, "B"),
			Row.of(1, "C"),
			Row.of(1, "D"),
			Row.of(2, "B"),
			Row.of(2, "C"),
			Row.of(2, "E"),
			Row.of(3, "A"),
			Row.of(3, "B"),
			Row.of(3, "C"),
			Row.of(3, "E"),
			Row.of(4, "B"),
			Row.of(4, "D"),
			Row.of(4, "E"),
			Row.of(5, "A"),
			Row.of(5, "B"),
			Row.of(5, "C"),
			Row.of(5, "D"),
		};

		BatchOperator data = new MemSourceBatchOp(rows, "id int, item string").orderBy("item", -1)
			.groupBy("id", "id,CONCAT_AGG(item) AS items");

		FpGrowthBatchOp fpGrowth = new FpGrowthBatchOp()
			.setItemsCol("items")
			.setMinSupportPercent(0.4)
			.setMinConfidence(0.6);

		fpGrowth.linkFrom(data);
		Assert.assertEquals(fpGrowth.count(), 19);
		Assert.assertEquals(fpGrowth.getSideOutput(0).count(), 27);
	}
}