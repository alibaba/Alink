package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.evaluation.ConfusionMatrix;
import com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.ReplaceLabelMapFunction;
import com.alibaba.alink.operator.common.evaluation.OutlierMetrics;
import com.alibaba.alink.operator.common.evaluation.OutlierMetricsSummary;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.evaluation.EvalOutlierStreamOp.CalcOutlierMetricsSummaryWindowFunction;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.INLIER_LABEL;
import static com.alibaba.alink.operator.common.evaluation.EvalOutlierUtils.OUTLIER_LABEL;

public class EvalOutlierStreamOpTest extends AlinkTestBase {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private static final double EPS = 1e-12;

	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		for (int i = 0; i < 10; i += 1) {    // 2 ^ 10 times
			rows = ArrayUtils.addAll(rows, rows);
		}

		MemSourceStreamOp source = new MemSourceStreamOp(rows, new String[] {"label", "detailInput"});

		EvalOutlierStreamOp op1 = new EvalOutlierStreamOp()
			.setLabelCol("label")
			.setOutlierValueStrings("prefix1")
			.setTimeInterval(0.001)
			.setPredictionDetailCol("detailInput")
			.linkFrom(source);

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op1);
		StreamOperator.execute();

		String[] realLabels = new String[] {"prefix2", "prefix1", "prefix0"};
		List <Row> results = sink.getAndRemoveValues();
		for (Row result : results) {
			String tag = (String) result.getField(0);
			OutlierMetrics outlierMetrics = new OutlierMetrics(Params.fromJson((String) result.getField(1)));
			Assert.assertTrue(Arrays.asList("prefix0", "prefix1")
				.containsAll(Arrays.asList(outlierMetrics.getOutlierValueArray())));
			// Not always have all labels in the window
			if (outlierMetrics.getLabelArray().length == realLabels.length) {
				Assert.assertArrayEquals(realLabels, outlierMetrics.getLabelArray());
			}
		}
	}

	@Test
	public void testWithNegInf() throws Exception {
		Row[] rows = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": \"-Infinity\", \"is_outlier\": false, \"lof\": 1.3}")
		};

		for (int i = 0; i < 10; i += 1) {    // 2 ^ 10 times
			rows = ArrayUtils.addAll(rows, rows);
		}

		MemSourceStreamOp source = new MemSourceStreamOp(rows, new String[] {"label", "detailInput"});

		EvalOutlierStreamOp op1 = new EvalOutlierStreamOp()
			.setLabelCol("label")
			.setOutlierValueStrings("prefix1")
			.setTimeInterval(0.001)
			.setPredictionDetailCol("detailInput")
			.linkFrom(source);

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op1);
		StreamOperator.execute();

		String[] realLabels = new String[] {"prefix2", "prefix1", "prefix0"};
		List <Row> results = sink.getAndRemoveValues();
		for (Row result : results) {
			String tag = (String) result.getField(0);
			OutlierMetrics outlierMetrics = new OutlierMetrics(Params.fromJson((String) result.getField(1)));
			Assert.assertTrue(Arrays.asList("prefix0", "prefix1")
				.containsAll(Arrays.asList(outlierMetrics.getOutlierValueArray())));
			// Not always have all labels in the window
			if (outlierMetrics.getLabelArray().length == realLabels.length) {
				Assert.assertArrayEquals(realLabels, outlierMetrics.getLabelArray());
			}
		}
	}

	@Test
	public void testNonStringOutliers() throws Exception {
		Row[] rows = new Row[] {
			Row.of(2, "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(2, "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(2, "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(1, "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(2, "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(1, "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(1, null),
			Row.of(2, "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of(1, "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of(2, "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of(1, "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of(1, "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of(2, "{\"outlier_score\": 0.1, \"is_outlier\": false, \"lof\": 1.3}")
		};

		for (int i = 0; i < 10; i += 1) {    // 2 ^ 10 times
			rows = ArrayUtils.addAll(rows, rows);
		}

		MemSourceStreamOp source = new MemSourceStreamOp(rows, new String[] {"label", "detailInput"});

		EvalOutlierStreamOp op1 = new EvalOutlierStreamOp()
			.setLabelCol("label")
			.setOutlierValueStrings("2")
			.setTimeInterval(0.001)
			.setPredictionDetailCol("detailInput")
			.linkFrom(source);

		CollectSinkStreamOp sink = new CollectSinkStreamOp()
			.linkFrom(op1);
		StreamOperator.execute();

		String[] realLabels = new String[] {"2", "1"};
		List <Row> results = sink.getAndRemoveValues();
		for (Row result : results) {
			OutlierMetrics outlierMetrics = new OutlierMetrics(Params.fromJson((String) result.getField(1)));
			Assert.assertArrayEquals(new String[] {"2"}, outlierMetrics.getOutlierValueArray());
			// Not always have all labels in the window
			if (outlierMetrics.getLabelArray().length == realLabels.length) {
				Assert.assertArrayEquals(realLabels, outlierMetrics.getLabelArray());
			}
		}
	}

	@Test
	public void testCalcOutlierStats() {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.1, \"is_outlier\": false, \"lof\": 1.3}")
		};
		Tuple4 <Integer, List <Tuple2 <Double, ConfusionMatrix>>, Double, Double> outlierStats =
			EvalOutlierStreamOp.calcOutlierStats(Arrays.asList(data), "prefix1");
		Assert.assertEquals(data.length - 1, (int) outlierStats.f0);
		Assert.assertEquals(0.55, outlierStats.f2, EPS);
		Assert.assertEquals(0.4, outlierStats.f3, EPS);
		List <Tuple2 <Double, ConfusionMatrix>> threshCMs = outlierStats.f1;
		Assert.assertEquals(data.length - 1, threshCMs.size());

		Assert.assertEquals(0.9, threshCMs.get(0).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}}), threshCMs.get(0).f1);

		Assert.assertEquals(0.1, threshCMs.get(threshCMs.size() - 1).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}), threshCMs.get(threshCMs.size() - 1).f1);

		Assert.assertEquals(0.65, threshCMs.get(4).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}}), threshCMs.get(4).f1);

		Assert.assertEquals(0.35, threshCMs.get(8).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}}), threshCMs.get(8).f1);
	}

	@Test
	public void testCalcOutlierStatsWithNegInf() {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": \"-Infinity\", \"is_outlier\": false, \"lof\": 1.3}")
		};
		Tuple4 <Integer, List <Tuple2 <Double, ConfusionMatrix>>, Double, Double> outlierStats =
			EvalOutlierStreamOp.calcOutlierStats(Arrays.asList(data), "prefix1");
		Assert.assertEquals(data.length - 1, (int) outlierStats.f0);
		Assert.assertEquals(0.55, outlierStats.f2, EPS);
		Assert.assertEquals(0.4, outlierStats.f3, EPS);
		List <Tuple2 <Double, ConfusionMatrix>> threshCMs = outlierStats.f1;
		Assert.assertEquals(data.length - 1, threshCMs.size());

		Assert.assertEquals(0.9, threshCMs.get(0).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}}), threshCMs.get(0).f1);

		Assert.assertEquals(Double.NEGATIVE_INFINITY, threshCMs.get(threshCMs.size() - 1).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}), threshCMs.get(threshCMs.size() - 1).f1);

		Assert.assertEquals(0.65, threshCMs.get(4).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}}), threshCMs.get(4).f1);

		Assert.assertEquals(0.35, threshCMs.get(8).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}}), threshCMs.get(8).f1);
	}

	@Test
	public void testCalcOutlierStatsWithPosInf() {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": \"Infinity\", \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": \"-Infinity\", \"is_outlier\": false, \"lof\": 1.3}")
		};
		Tuple4 <Integer, List <Tuple2 <Double, ConfusionMatrix>>, Double, Double> outlierStats =
			EvalOutlierStreamOp.calcOutlierStats(Arrays.asList(data), "prefix1");
		Assert.assertEquals(data.length - 1, (int) outlierStats.f0);
		Assert.assertEquals(0.55, outlierStats.f2, EPS);
		Assert.assertEquals(0.4, outlierStats.f3, EPS);
		List <Tuple2 <Double, ConfusionMatrix>> threshCMs = outlierStats.f1;
		Assert.assertEquals(data.length - 1, threshCMs.size());

		Assert.assertEquals(Double.POSITIVE_INFINITY, threshCMs.get(0).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}}), threshCMs.get(0).f1);

		Assert.assertEquals(Double.NEGATIVE_INFINITY, threshCMs.get(threshCMs.size() - 1).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}), threshCMs.get(threshCMs.size() - 1).f1);

		Assert.assertEquals(0.65, threshCMs.get(4).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}}), threshCMs.get(4).f1);

		Assert.assertEquals(0.35, threshCMs.get(8).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}}), threshCMs.get(8).f1);
	}

	@Test
	public void testMergedConfusionMatrixLists() {
		List <Tuple2 <Double, ConfusionMatrix>> list0 = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}})),
			Tuple2.of(0.1, new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}))
		);
		List <Tuple2 <Double, ConfusionMatrix>> list1 = Arrays.asList(
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{1, 0}, {4, 4}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{2, 1}, {3, 3}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{3, 3}, {2, 1}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{5, 4}, {0, 0}}))
		);

		List <Tuple2 <Double, ConfusionMatrix>> expected = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{2, 0}, {10, 9}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{3, 1}, {9, 8}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{6, 2}, {6, 7}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{11, 7}, {1, 2}})),
			Tuple2.of(0.1, new ConfusionMatrix(new long[][] {{12, 9}, {0, 0}}))
		);

		//noinspection unchecked
		List <Tuple2 <Double, ConfusionMatrix>> merged = OutlierMetricsSummary.mergeConfusionMatrixLists(list0, list1);
		Assert.assertEquals(expected.size(), merged.size());
		for (int i = 0; i < expected.size(); i += 1) {
			Assert.assertEquals(expected.get(i).f0, merged.get(i).f0);
			Assert.assertEquals(expected.get(i).f1, merged.get(i).f1);
		}
	}

	@Test
	public void testMergedConfusionMatrixListsWithNegInf() {
		List <Tuple2 <Double, ConfusionMatrix>> list0 = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}})),
			Tuple2.of(Double.NEGATIVE_INFINITY, new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}))
		);
		List <Tuple2 <Double, ConfusionMatrix>> list1 = Arrays.asList(
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{1, 0}, {4, 4}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{2, 1}, {3, 3}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{3, 3}, {2, 1}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{5, 4}, {0, 0}}))
		);

		List <Tuple2 <Double, ConfusionMatrix>> expected = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{2, 0}, {10, 9}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{3, 1}, {9, 8}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{6, 2}, {6, 7}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{11, 7}, {1, 2}})),
			Tuple2.of(Double.NEGATIVE_INFINITY, new ConfusionMatrix(new long[][] {{12, 9}, {0, 0}}))
		);

		//noinspection unchecked
		List <Tuple2 <Double, ConfusionMatrix>> merged = OutlierMetricsSummary.mergeConfusionMatrixLists(list0, list1);
		Assert.assertEquals(expected.size(), merged.size());
		for (int i = 0; i < expected.size(); i += 1) {
			Assert.assertEquals(expected.get(i).f0, merged.get(i).f0);
			Assert.assertEquals(expected.get(i).f1, merged.get(i).f1);
		}
	}

	@Test
	public void testMergedConfusionMatrixListsWithPosInf() {
		List <Tuple2 <Double, ConfusionMatrix>> list0 = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}})),
			Tuple2.of(Double.NEGATIVE_INFINITY, new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}))
		);
		List <Tuple2 <Double, ConfusionMatrix>> list1 = Arrays.asList(
			Tuple2.of(Double.POSITIVE_INFINITY, new ConfusionMatrix(new long[][] {{1, 0}, {4, 4}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{2, 1}, {3, 3}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{3, 3}, {2, 1}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{5, 4}, {0, 0}}))
		);

		List <Tuple2 <Double, ConfusionMatrix>> expected = Arrays.asList(
			Tuple2.of(Double.POSITIVE_INFINITY, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{2, 0}, {10, 9}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{3, 1}, {9, 8}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{6, 2}, {6, 7}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{11, 7}, {1, 2}})),
			Tuple2.of(Double.NEGATIVE_INFINITY, new ConfusionMatrix(new long[][] {{12, 9}, {0, 0}}))
		);

		//noinspection unchecked
		List <Tuple2 <Double, ConfusionMatrix>> merged = OutlierMetricsSummary.mergeConfusionMatrixLists(list0, list1);
		Assert.assertEquals(expected.size(), merged.size());
		for (int i = 0; i < expected.size(); i += 1) {
			Assert.assertEquals(expected.get(i).f0, merged.get(i).f0);
			Assert.assertEquals(expected.get(i).f1, merged.get(i).f1);
		}
	}

	@Test
	public void testFilterCloseEntries() {
		List <Tuple2 <Double, ConfusionMatrix>> l = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{2, 0}, {10, 9}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{3, 1}, {9, 8}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{6, 2}, {6, 7}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{11, 7}, {1, 2}})),
			Tuple2.of(0.1, new ConfusionMatrix(new long[][] {{12, 9}, {0, 0}}))
		);
		List <Tuple2 <Double, ConfusionMatrix>> filtered = OutlierMetricsSummary.filterCloseEntries(l, 4);
		List <Tuple2 <Double, ConfusionMatrix>> expected = Arrays.asList(
			Tuple2.of(0.9, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{3, 1}, {9, 8}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{11, 7}, {1, 2}}))
		);
		Assert.assertEquals(expected.size(), filtered.size());
		for (int i = 0; i < expected.size(); i += 1) {
			Assert.assertEquals(expected.get(i).f0, filtered.get(i).f0);
			Assert.assertEquals(expected.get(i).f1, filtered.get(i).f1);
		}
	}

	@Test
	public void testFilterCloseEntriesWithInf() {
		List <Tuple2 <Double, ConfusionMatrix>> l = Arrays.asList(
			Tuple2.of(Double.POSITIVE_INFINITY, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{2, 0}, {10, 9}})),
			Tuple2.of(0.7, new ConfusionMatrix(new long[][] {{3, 1}, {9, 8}})),
			Tuple2.of(0.65, new ConfusionMatrix(new long[][] {{6, 2}, {6, 7}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(0.15, new ConfusionMatrix(new long[][] {{11, 7}, {1, 2}})),
			Tuple2.of(Double.NEGATIVE_INFINITY, new ConfusionMatrix(new long[][] {{12, 9}, {0, 0}}))
		);
		List <Tuple2 <Double, ConfusionMatrix>> filtered = OutlierMetricsSummary.filterCloseEntries(l, 4);
		List <Tuple2 <Double, ConfusionMatrix>> expected = Arrays.asList(
			Tuple2.of(Double.POSITIVE_INFINITY, new ConfusionMatrix(new long[][] {{1, 0}, {11, 9}})),
			Tuple2.of(0.8, new ConfusionMatrix(new long[][] {{2, 0}, {10, 9}})),
			Tuple2.of(0.35, new ConfusionMatrix(new long[][] {{9, 6}, {3, 3}})),
			Tuple2.of(Double.NEGATIVE_INFINITY, new ConfusionMatrix(new long[][] {{12, 9}, {0, 0}}))
		);
		Assert.assertEquals(expected.size(), filtered.size());
		for (int i = 0; i < expected.size(); i += 1) {
			Assert.assertEquals(expected.get(i).f0, filtered.get(i).f0);
			Assert.assertEquals(expected.get(i).f1, filtered.get(i).f1);
		}
	}

	@Test
	public void testCalcOutlierMetricsSummaryWindowFunction() throws Exception {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.1, \"is_outlier\": false, \"lof\": 1.3}")
		};
		ReplaceLabelMapFunction replaceLabelMapFunction = new ReplaceLabelMapFunction(
			new HashSet <>(Collections.singletonList("prefix1")), 1, 0);
		data = Arrays.stream(data)
			.map(replaceLabelMapFunction::map)
			.toArray(Row[]::new);

		String[] outlierValueStrings = new String[] {"prefix1"};
		CalcOutlierMetricsSummaryWindowFunction f = new CalcOutlierMetricsSummaryWindowFunction(outlierValueStrings);
		List <OutlierMetricsSummary> summaryList = new ArrayList <>();
		ListCollector <OutlierMetricsSummary> collector = new ListCollector <>(summaryList);
		f.apply(null, Arrays.asList(data), collector);
		Assert.assertEquals(1, summaryList.size());
		OutlierMetricsSummary summary = summaryList.get(0);

		Assert.assertEquals(data.length - 1, summary.getTotal());
		Assert.assertEquals(0.55, summary.getMinOutlierScore(), EPS);
		Assert.assertEquals(0.4, summary.getMaxNormalScore(), EPS);
		Assert.assertArrayEquals(new String[] {"prefix1", "prefix0"}, summary.getAllValueStrings());
		Assert.assertArrayEquals(new String[] {"prefix1"}, summary.getOutlierValueStrings());
		Assert.assertArrayEquals(new String[] {OUTLIER_LABEL, INLIER_LABEL}, summary.getLabels());

		List <Tuple2 <Double, ConfusionMatrix>> threshCMs = summary.getThreshCMs();
		Assert.assertEquals(data.length - 1, threshCMs.size());

		Assert.assertEquals(0.9, threshCMs.get(0).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{1, 0}, {6, 5}}), threshCMs.get(0).f1);

		Assert.assertEquals(0.1, threshCMs.get(threshCMs.size() - 1).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{7, 5}, {0, 0}}), threshCMs.get(threshCMs.size() - 1).f1);

		Assert.assertEquals(0.65, threshCMs.get(4).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{4, 1}, {3, 4}}), threshCMs.get(4).f1);

		Assert.assertEquals(0.35, threshCMs.get(8).f0, EPS);
		Assert.assertEquals(new ConfusionMatrix(new long[][] {{6, 3}, {1, 2}}), threshCMs.get(8).f1);
	}

	@Test
	public void testToMetrics() throws IOException {
		Row[] data = new Row[] {
			Row.of("prefix1", "{\"outlier_score\": 0.9, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.8, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.7, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.75, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.65, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.6, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", null),
			Row.of("prefix1", "{\"outlier_score\": 0.55, \"is_outlier\": true, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.4, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.35, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.3, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix0", "{\"outlier_score\": 0.2, \"is_outlier\": false, \"lof\": 1.3}"),
			Row.of("prefix1", "{\"outlier_score\": 0.1, \"is_outlier\": false, \"lof\": 1.3}")
		};
		Tuple4 <Integer, List <Tuple2 <Double, ConfusionMatrix>>, Double, Double> outlierStats =
			EvalOutlierStreamOp.calcOutlierStats(Arrays.asList(data), "prefix1");

		Object[] labelValueStrs = new Object[] {OUTLIER_LABEL, INLIER_LABEL};
		String[] outlierValueStrings = new String[] {"prefix1"};
		String[] allLabelStrs = new String[] {"prefix1", "prefix0"};

		OutlierMetricsSummary summary = new OutlierMetricsSummary(outlierStats.f0, labelValueStrs,
			allLabelStrs, outlierValueStrings, outlierStats.f2, outlierStats.f3, outlierStats.f1);
		OutlierMetrics metrics = summary.toMetrics();

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
		metrics.saveRecallPrecisionCurveAsImage(folder.getRoot().toPath() + "recallPrecision.png", true);
		metrics.saveLorenzCurveAsImage(folder.getRoot().toPath() + "lorenzCurve.png", true);
	}
}
