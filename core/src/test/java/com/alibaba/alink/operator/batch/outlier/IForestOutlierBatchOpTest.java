package com.alibaba.alink.operator.batch.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalOutlierBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class IForestOutlierBatchOpTest extends AlinkTestBase {

	@Test
	public void test() throws Exception {

		BatchOperator <?> data = new MemSourceBatchOp(
			new Object[][] {
				{0.73, 0},
				{0.24, 0},
				{0.63, 0},
				{0.55, 0},
				{0.73, 0},
				{0.41, 0},
			},
			new String[]{"val", "label"});

		BatchOperator <?> outlier = new IForestOutlierBatchOp()
			.setFeatureCols("val")
			.setOutlierThreshold(3.0)
			.setPredictionCol("pred")
			.setPredictionDetailCol("pred_detail");

		EvalOutlierBatchOp eval = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("pred_detail")
			.setOutlierValueStrings("1");

		OutlierMetrics metrics = data
			.link(outlier)
			.link(eval)
			.collectMetrics();

		Assert.assertEquals(1.0, metrics.getAccuracy(), 10e-6);

	}
}