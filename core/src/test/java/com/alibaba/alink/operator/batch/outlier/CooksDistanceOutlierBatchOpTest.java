package com.alibaba.alink.operator.batch.outlier;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CooksDistanceOutlierBatchOpTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {
		BatchOperator <?> data = new MemSourceBatchOp(
			new Object[][] {
				{4, -3},
				{2, -2},
				{3, -1},
				{0, 0},
				{-1, 1},
				{-2, 2},
				{-5, 3}
			},
			new String[] {"label", "val"});

		BatchOperator <?> outlier = new CooksDistanceOutlierBatchOp()
			.setFeatureCols("val")
			.setLabelCol("label")
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		data.link(outlier);

		List <Row> results = outlier.select("pred").collect();

		Assert.assertFalse((boolean)results.get(0).getField(0));
	}

}