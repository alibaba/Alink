package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BaseSimpleMultiLabelMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Test for EvalMultiLabel.
 */

public class EvalMultiLabelBatchOpTest extends AlinkTestBase {
	private Row[] rows = new Row[] {
		Row.of("{\"object\":\"[0.0, 1.0]\"}", "{\"object\":\"[0.0, 2.0]\"}"),
		Row.of("{\"object\":\"[0.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
		Row.of("{\"object\":\"[]\"}", "{\"object\":\"[0.0]\"}"),
		Row.of("{\"object\":\"[2.0]\"}", "{\"object\":\"[2.0]\"}"),
		Row.of("{\"object\":\"[2.0, 0.0]\"}", "{\"object\":\"[2.0, 0.0]\"}"),
		Row.of("{\"object\":\"[0.0, 1.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
		Row.of("{\"object\":\"[1.0]\"}", "{\"object\":\"[1.0, 2.0]\"}")
	};

	@Test
	public void test() throws Exception {
		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows), new String[] {"pred", "label"});

		EvalMultiLabelBatchOp op = new EvalMultiLabelBatchOp()
			.setLabelCol("label")
			.setPredictionCol("pred")
			.linkFrom(data);

		BaseSimpleMultiLabelMetrics metrics = op.collectMetrics();
		Assert.assertEquals(metrics.getRecall(), 0.64, 0.01);
		Assert.assertEquals(metrics.getPrecision(), 0.66, 0.01);
		Assert.assertEquals(metrics.getAccuracy(), 0.54, 0.01);
		Assert.assertEquals(metrics.getF1(), 0.63, 0.01);
		Assert.assertEquals(metrics.getMicroF1(), 0.69, 0.01);
		Assert.assertEquals(metrics.getMicroPrecision(), 0.72, 0.01);
		Assert.assertEquals(metrics.getMicroRecall(), 0.66, 0.01);
	}

}