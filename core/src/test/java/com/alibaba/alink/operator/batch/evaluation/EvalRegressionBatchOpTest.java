package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for RegressionEvaluation
 */
public class EvalRegressionBatchOpTest {
	@Test
	public void test() throws Exception {
		Row[] data =
			new Row[] {
				Row.of(0.4, 0.5),
				Row.of(0.3, 0.5),
				Row.of(0.2, 0.6),
				Row.of(0.6, 0.7),
				Row.of(0.1, 0.5)
			};

		MemSourceBatchOp input = new MemSourceBatchOp(data, new String[] {"label", "pred"});

		RegressionMetrics metrics = new EvalRegressionBatchOp()
			.setLabelCol("label")
			.setPredictionCol("pred")
			.linkFrom(input)
			.collectMetrics();

		Assert.assertEquals(0.275, metrics.getRmse(), 0.01);
		Assert.assertEquals(-1.56, metrics.getR2(), 0.01);
		Assert.assertEquals(0.38, metrics.getSse(), 0.01);
		Assert.assertEquals(141.66, metrics.getMape(), 0.01);
		Assert.assertEquals(0.24, metrics.getMae(), 0.01);
		Assert.assertEquals(0.32, metrics.getSsr(), 0.01);
		Assert.assertEquals(0.14, metrics.getSst(), 0.01);
	}

}