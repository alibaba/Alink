package com.alibaba.alink.operator.batch.associationrule;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class GroupedFpGrowthBatchOpTest extends AlinkTestBase {
	@Test
	public void linkFrom() throws Exception {
		Row[] rows = new Row[] {
			Row.of("a", "A,B,C,D"),
			Row.of("a", "B,C,E"),
			Row.of("a", "A,B,C,E"),
			Row.of("a", "B,D,E"),
			Row.of("a", "A,B,C,D"),
			Row.of("b", "A,B,C,D"),
			Row.of("b", "B,C,E"),
			Row.of("b", "A,B,C,E"),
			Row.of("b", "B,D,E"),
			Row.of("b", "A,B,C,D"),
		};

		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"key", "items"});

		BatchOperator fpgrowth = new GroupedFpGrowthBatchOp()
			.setGroupCol("key")
			.setItemsCol("items")
			.setMinSupportPercent(0.4)
			.setMinConfidence(0.6);

		fpgrowth.linkFrom(data);
		Assert.assertEquals(fpgrowth.count(), 38);
	}

	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("changjiang", 1, "A"),
			Row.of("changjiang", 1, "B"),
			Row.of("changjiang", 1, "C"),
			Row.of("changjiang", 1, "D"),
			Row.of("changjiang", 2, "B"),
			Row.of("changjiang", 2, "C"),
			Row.of("changjiang", 2, "E"),
			Row.of("huanghe", 3, "A"),
			Row.of("huanghe", 3, "B"),
			Row.of("huanghe", 3, "C"),
			Row.of("huanghe", 3, "E"),
			Row.of("huanghe", 4, "B"),
			Row.of("huanghe", 4, "D"),
			Row.of("huanghe", 4, "E"),
			Row.of("huanghe", 5, "A"),
			Row.of("huanghe", 5, "B"),
			Row.of("huanghe", 5, "C"),
			Row.of("huanghe", 5, "D"),
		};

		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), "groupid string, id int, item string")
			.groupBy("groupid,id", "groupid,id,CONCAT_AGG(item) AS items");

		BatchOperator fpgrowth = new GroupedFpGrowthBatchOp()
			.setGroupCol("groupid")
			.setItemsCol("items")
			.setMinSupportCount(2);

		fpgrowth.linkFrom(data);
		Assert.assertEquals(fpgrowth.count(), 14);
		Assert.assertEquals(fpgrowth.getSideOutput(0).count(), 15);
	}
}