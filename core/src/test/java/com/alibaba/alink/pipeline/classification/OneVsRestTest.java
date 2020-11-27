package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OneVsRestTest extends AlinkTestBase {
	private BatchOperator data;

	@Before
	public void setup() throws Exception {
		data = Iris.getBatchData();
	}

	@Test
	public void lrTriCls() throws Exception {
		LogisticRegression lr = new LogisticRegression()
			.setFeatureCols(Iris.getFeatureColNames())
			.setLabelCol(Iris.getLabelColName())
			.setPredictionDetailCol("pred_detail")
			.setMaxIter(100);

		OneVsRest oneVsRest = new OneVsRest()
			.setClassifier(lr).setNumClass(3)
			.setPredictionCol("pred_label");

		OneVsRestModel model = oneVsRest.fit(data);
		model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail");
		BatchOperator pred = model.transform(data);

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setLabelCol(Iris.getLabelColName())
			.linkFrom(pred)
			.collectMetrics();
		Assert.assertTrue(metrics.getAccuracy() > 0.9);
	}

	@Test
	public void gbdtTriCls() throws Exception {
		GbdtClassifier gbdt = new GbdtClassifier()
			.setFeatureCols(Iris.getFeatureColNames())
			.setLabelCol(Iris.getLabelColName())
			.setCategoricalCols()
			.setMaxBins(128)
			.setMaxDepth(5)
			.setNumTrees(10)
			.setMinSamplesPerLeaf(1)
			.setLearningRate(0.3)
			.setMinInfoGain(0.0)
			.setSubsamplingRatio(1.0)
			.setFeatureSubsamplingRatio(1.0);

		OneVsRest oneVsRest = new OneVsRest().setClassifier(gbdt).setNumClass(3);
		OneVsRestModel model = oneVsRest.fit(data);
		model.setPredictionCol("pred_result").setPredictionDetailCol("pred_detail");
		BatchOperator pred = model.transform(data);

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setLabelCol(Iris.getLabelColName())
			.linkFrom(pred)
			.collectMetrics();
		Assert.assertTrue(metrics.getAccuracy() > 0.9);
	}
}
