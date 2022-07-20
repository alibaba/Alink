package com.alibaba.alink.operator.batch.evaluation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkFlinkExecutionErrorException;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;

public class EvalOutlierBatchOpTest extends AlinkTestBase {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testScoreIsProb() throws Exception {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("prefix1")
			.linkFrom(source)
			.collectMetrics();

		System.out.println(metrics);

		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"prefix1"}, metrics.getOutlierValueArray());
		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314,
			metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}

	@Test
	public void testScoreIsNotProb() throws Exception {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 1.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 1.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": -1.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": -1.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("prefix1")
			.linkFrom(source)
			.collectMetrics();

		System.out.println(metrics);

		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"prefix1"}, metrics.getOutlierValueArray());
		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314,
			metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}

	@Test
	public void testNonStringOutliers() throws Exception {
		Row[] data =
			new Row[] {
				Row.of(2, "{\"outlier_score\": 1.9, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(2, "{\"outlier_score\": 1.8, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(2, "{\"outlier_score\": 1.7, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(1, "{\"outlier_score\": 1.75, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(1, "{\"outlier_score\": 1.6, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(2, "{\"outlier_score\": 1.65, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(1, null),
				Row.of(2, "{\"outlier_score\": 1.55, \"is_outlier\": true, \"lof\": 1.3}"),
				Row.of(1, "{\"outlier_score\": 1.4, \"is_outlier\": false, \"lof\": 1.3}"),
				Row.of(1, "{\"outlier_score\": -1.3, \"is_outlier\": false, \"lof\": 1.3}"),
				Row.of(2, "{\"outlier_score\": -1.35, \"is_outlier\": false, \"lof\": 1.3}"),
				Row.of(1, "{\"outlier_score\": -1.2, \"is_outlier\": false, \"lof\": 1.3}"),
				Row.of(2, "{\"outlier_score\": -1.1, \"is_outlier\": false, \"lof\": 1.3}")
			};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("2")
			.linkFrom(source)
			.collectMetrics();

		System.out.println(metrics);

		Assert.assertArrayEquals(new String[] {"2", "1"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"2"}, metrics.getOutlierValueArray());
		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314,
			metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}

	@Test
	public void testEmptyInput() throws Exception {
		Row[] rows = new Row[] {
			Row.of(null, null),
			Row.of("1", null),
			Row.of(null, "1")
		};

		MemSourceBatchOp data = new MemSourceBatchOp(Arrays.asList(rows),
			new TableSchema(new String[] {"pred", "label"},
				new TypeInformation[] {Types.STRING, Types.STRING}));
		try {
			EvalOutlierBatchOp op = new EvalOutlierBatchOp()
				.setLabelCol("label")
				.setPredictionDetailCol("pred")
				.setOutlierValueStrings("1")
				.linkFrom(data);
			op.print();
			Assert.fail("Expected an IllegalStateException to be thrown");
		} catch (JobExecutionException | ProgramInvocationException | AkFlinkExecutionErrorException e) {
			// pass
		}

	}

	@Test
	public void testNegInfScore() throws IOException {
		// Modify the smallest score to -Infinity should not change metrics
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 1.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 1.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": \"-Infinity\", \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": -1.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("prefix1")
			.linkFrom(source)
			.collectMetrics();

		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"prefix1"}, metrics.getOutlierValueArray());
		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}

	@Test
	public void testPosInfScore() throws IOException {
		// Modify the largest score to Infinity should not change metrics
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": \"Infinity\", \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 1.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": \"-Infinity\", \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": -1.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("prefix1")
			.linkFrom(source)
			.collectMetrics();

		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"prefix1"}, metrics.getOutlierValueArray());
		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(0.666, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0.314, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.666, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.666, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}

	@Test
	public void testUnmatchedScorePrediction() throws IOException {
		// outlier_score and is_outlier are independent
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 1.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.7, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.5, \"is_outlier\": false, \"lof\": 1.3}"),
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("prefix1")
			.linkFrom(source)
			.collectMetrics();

		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"prefix1"}, metrics.getOutlierValueArray());
		Assert.assertEquals(1., metrics.getPrc(), 0.01);
		Assert.assertEquals(1., metrics.getKs(), 0.01);
		Assert.assertEquals(1., metrics.getAuc(), 0.01);
		Assert.assertEquals(0.5, metrics.getAccuracy(), 0.01);
		Assert.assertEquals(0., metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(0.5, metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(0.5, metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}

	@Test
	public void testUnmatchedScorePredictionLarge() throws Exception {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 1.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.8, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.75, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 1.65, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 1.55, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 1.4, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": -1.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": -1.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": -1.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		MemSourceBatchOp source = new MemSourceBatchOp(data, new String[] {"label", "detailInput"});

		OutlierMetrics metrics = new EvalOutlierBatchOp()
			.setLabelCol("label")
			.setPredictionDetailCol("detailInput")
			.setOutlierValueStrings("prefix1")
			.linkFrom(source)
			.collectMetrics();

		System.out.println(metrics);

		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, metrics.getLabelArray());
		Assert.assertArrayEquals(new String[] {"prefix1"}, metrics.getOutlierValueArray());
		Assert.assertEquals(0.769, metrics.getPrc(), 0.01);
		Assert.assertEquals(0.371, metrics.getKs(), 0.01);
		Assert.assertEquals(0.657, metrics.getAuc(), 0.01);
		Assert.assertEquals(5. / 12., metrics.getAccuracy(), 0.01);
		Assert.assertEquals(-0.105, metrics.getMacroKappa(), 0.01);
		Assert.assertEquals(5. / 12., metrics.getMicroPrecision(), 0.01);
		Assert.assertEquals(5. / 12., metrics.getWeightedRecall(), 0.01);
		metrics.saveRocCurveAsImage(folder.getRoot().toPath() + "rocCurve.png", true);
		metrics.saveKSAsImage(folder.getRoot().toPath() + "ks.png", true);
		metrics.saveLiftChartAsImage(folder.getRoot().toPath() + "liftchart.png", true);
		metrics.savePrecisionRecallCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}
}
