package com.alibaba.alink.pipeline.classification;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.testhttpsrc.Iris;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.MultilayerPerceptronPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class MultilayerPerceptronClassifierTest extends AlinkTestBase {

	@Test
	public void testMLPC() {
		BatchOperator<?> data = Iris.getBatchData();

		MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
			.setFeatureCols(Iris.getFeatureColNames())
			.setLabelCol(Iris.getLabelColName())
			.setLayers(new int[] {4, 5, 3})
			.setMaxIter(100)
			.setPredictionCol("pred_label")
			.setPredictionDetailCol("pred_detail");

		BatchOperator<?> res = classifier.fit(data).transform(data);

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setLabelCol(Iris.getLabelColName())
			.linkFrom(res)
			.collectMetrics();

		Assert.assertTrue(metrics.getAccuracy() > 0.9);

	}

	@Test
	public void testMLPCV1() throws Exception {
		BatchOperator<?> data = Iris.getBatchData();

		MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
			.setFeatureCols(Iris.getFeatureColNames())
			.setLabelCol(Iris.getLabelColName())
			.setLayers(new int[] {4, 5, 3})
			.setMaxIter(100)
			.setPredictionCol("pred_label");

		classifier.fit(data).transform(data).lazyPrint(1);
		BatchOperator.execute();

	}

	@Test
	public void testDenseMLPC() {

		Row[] array = new Row[] {
			Row.of("$31$01.0 11.0 21.0 301.0", "1.0 1.0 1.0 1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of("$31$01.0 11.0 20.0 301.0", "1.0 1.0 0.0 1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of("$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};
		String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "label"};

		BatchOperator<?> data = new MemSourceBatchOp(Arrays.asList(array), veccolNames);

		MultilayerPerceptronClassifier classifier = new MultilayerPerceptronClassifier()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setLayers(new int[] {4, 5, 2})
			.setMaxIter(100)
			.setPredictionCol("pred_label")
			.setPredictionDetailCol("pred_detail");

		BatchOperator<?> res = classifier.fit(data).transform(data);

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setLabelCol("label")
			.linkFrom(res)
			.collectMetrics();

		Assert.assertTrue(metrics.getAccuracy() > 0.9);

	}

	@Test
	public void testIncrementalMLPC() {
		AlinkGlobalConfiguration.setPrintProcessInfo(true);
		Row[] array = new Row[] {
			Row.of("$31$01.0 11.0 21.0 301.0", "1.0 1.0 1.0 1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of("$31$01.0 11.0 20.0 301.0", "1.0 1.0 0.0 1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of("$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$01.0 10.0 21.0 301.0", "1.0 0.0 1.0 1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$00.0 11.0 21.0 300.0", "0.0 1.0 1.0 0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};
		String[] veccolNames = new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "label"};

		BatchOperator<?> data = new MemSourceBatchOp(Arrays.asList(array), veccolNames);

		MultilayerPerceptronTrainBatchOp classifier = new MultilayerPerceptronTrainBatchOp()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setLayers(new int[] {4, 5, 2})
			.setMaxIter(5);

		BatchOperator<?> model = classifier.linkFrom(data);

		BatchOperator finalModel = new MultilayerPerceptronTrainBatchOp()
			.setVectorCol("vec")
			.setLabelCol("label")
			.setLayers(new int[] {4, 5, 2})
			.setMaxIter(5).linkFrom(data, model);


		BatchOperator<?> res = new MultilayerPerceptronPredictBatchOp().setPredictionDetailCol("pred_detail")
			.setPredictionCol("pred").linkFrom(finalModel, data);

		MultiClassMetrics metrics = new EvalMultiClassBatchOp()
			.setPredictionDetailCol("pred_detail")
			.setLabelCol("label")
			.linkFrom(res)
			.collectMetrics();

		Assert.assertTrue(metrics.getAccuracy() > 0.9);

	}
}