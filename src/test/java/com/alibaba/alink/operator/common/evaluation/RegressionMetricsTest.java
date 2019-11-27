package com.alibaba.alink.operator.common.evaluation;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for RegressionMetrics.
 */
public class RegressionMetricsTest {

	@Test
	public void reduceTest(){
		RegressionMetricsSummary regressionSummary1 = new RegressionMetricsSummary();
		regressionSummary1.total = 5;
		regressionSummary1.ySumLocal = 1.6;
		regressionSummary1.ySum2Local = 0.66;
		regressionSummary1.predSumLocal = 2.8;
		regressionSummary1.predSum2Local = 1.599;
		regressionSummary1.sseLocal = 0.38;
		regressionSummary1.maeLocal = 1.2;
		regressionSummary1.mapeLocal = 7.08;

		RegressionMetricsSummary regressionSummary2 = new RegressionMetricsSummary();
		regressionSummary2.total = 4;
		regressionSummary2.ySumLocal = 2.4;
		regressionSummary2.ySum2Local = 0.12;
		regressionSummary2.predSumLocal = 3.4;
		regressionSummary2.predSum2Local = 0.666;
		regressionSummary2.sseLocal = 0.5;
		regressionSummary2.maeLocal = 1.6;
		regressionSummary2.mapeLocal = 6.07;

		RegressionMetricsSummary metrics = regressionSummary1.merge(regressionSummary2);

		Assert.assertEquals(metrics.total, 9);
		Assert.assertEquals(metrics.ySumLocal, 4.0, 0.001);
		Assert.assertEquals(metrics.ySum2Local, 0.78, 0.001);
		Assert.assertEquals(metrics.predSumLocal, 6.2, 0.001);
		Assert.assertEquals(metrics.predSum2Local, 2.265, 0.001);
		Assert.assertEquals(metrics.sseLocal, 0.88, 0.001);
		Assert.assertEquals(metrics.maeLocal, 2.8, 0.001);
		Assert.assertEquals(metrics.mapeLocal, 13.15, 0.001);

		Assert.assertEquals(regressionSummary1.merge(null), regressionSummary1);
	}

	@Test
	public void saveAsParamsTest() {
		RegressionMetricsSummary regressionSummary1 = new RegressionMetricsSummary();
		regressionSummary1.total = 5;
		regressionSummary1.ySumLocal = 1.6;
		regressionSummary1.ySum2Local = 0.66;
		regressionSummary1.predSumLocal = 2.8;
		regressionSummary1.predSum2Local = 1.599;
		regressionSummary1.sseLocal = 0.38;
		regressionSummary1.maeLocal = 1.2;
		regressionSummary1.mapeLocal = 7.08;
		RegressionMetrics metrics = regressionSummary1.toMetrics();

		Assert.assertEquals(-1.56, metrics.getR2(), 0.01);
		Assert.assertEquals(0.38, metrics.getSse(), 0.01);
		Assert.assertEquals(141.6, metrics.getMape(), 0.01);
		Assert.assertEquals(0.27, metrics.getRmse(), 0.01);
		Assert.assertEquals(0.24, metrics.getMae(), 0.01);
		Assert.assertEquals(0.31, metrics.getSsr(), 0.01);
		Assert.assertEquals(0.14, metrics.getSst(), 0.01);
		Assert.assertEquals(1.2, metrics.getSae(), 0.01);
		Assert.assertEquals(0.06, metrics.getExplainedVariance(), 0.01);
	}
}