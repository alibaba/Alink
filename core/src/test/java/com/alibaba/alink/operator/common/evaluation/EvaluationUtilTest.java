package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getRankingMetrics;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getRegressionStatistics;

/**
 * Test for EvaluationConst.
 */

public class EvaluationUtilTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void getRegressionStatisticsTest() {
		Row[] rows =
			new Row[] {
				Row.of(0.4, 0.5),
				Row.of(0.3, 0.5),
				Row.of(0.4, null),
				Row.of(0.2, 0.6),
				Row.of(0.6, 0.7),
				Row.of(0.1, 0.5)
			};

		RegressionMetricsSummary metricsSummary = getRegressionStatistics(Arrays.asList(rows));
		Assert.assertEquals(metricsSummary.total, 5);
		Assert.assertEquals(metricsSummary.ySumLocal, 1.6, 0.001);
		Assert.assertEquals(metricsSummary.ySum2Local, 0.66, 0.001);
		Assert.assertEquals(metricsSummary.predSumLocal, 2.8, 0.001);
		Assert.assertEquals(metricsSummary.predSum2Local, 1.599, 0.001);
		Assert.assertEquals(metricsSummary.sseLocal, 0.38, 0.001);
		Assert.assertEquals(metricsSummary.maeLocal, 1.2, 0.001);
		Assert.assertEquals(metricsSummary.mapeLocal, 7.083, 0.001);
	}

	@Test
	public void getDetailStatisticsBinary() {
		Row[] rows =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"),
				Row.of(null, "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
			};
		BaseMetricsSummary baseMetric = getDetailStatistics(Arrays.asList(rows), null, true, Types.STRING);
		assertBinaryMetrics(baseMetric);

		Map <Object, Integer> map = new HashMap <>();
		map.put("prefix0", 1);
		map.put("prefix1", 0);

		baseMetric = getDetailStatistics(Arrays.asList(rows), true, Tuple2.of(map, new Object[] {"prefix1",
				"prefix0"}),
			Types.STRING);
		assertBinaryMetrics(baseMetric);

		baseMetric = getDetailStatistics(Arrays.asList(rows), "prefix0", true, Types.STRING);
		Assert.assertTrue(baseMetric instanceof BinaryMetricsSummary);
		BinaryMetricsSummary metrics = (BinaryMetricsSummary) baseMetric;

		Assert.assertArrayEquals(new Object[] {"prefix0", "prefix1"}, metrics.labels);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Not contain positiveValue");
		getDetailStatistics(Arrays.asList(rows), "0", true, Types.STRING);
	}

	@Test
	public void getDetailStatisticsBinaryDouble() {
		Row[] rows =
			new Row[] {
				Row.of(1.0, "{\"1.00\": 0.9, \"0.00\": 0.1}"),
				Row.of(1.00, "{\"1.0\": 0.8, \"0.00\": 0.2}"),
				Row.of(1.0, "{\"1.00\": 0.7, \"0.000\": 0.3}"),
				Row.of(0.0, "{\"1.0\": 0.75, \"0.0\": 0.25}"),
				Row.of(null, "{\"1.0\": 0.75, \"0.0\": 0.25}"),
				Row.of(0.00, "{\"1.0\": 0.6, \"0.00\": 0.4}")
			};
		BaseMetricsSummary baseMetric = getDetailStatistics(Arrays.asList(rows), null, true, Types.DOUBLE);
		assertBinaryMetrics(baseMetric);

		Map <Object, Integer> map = new HashMap <>();
		map.put(0.0, 1);
		map.put(1.0, 0);

		baseMetric = getDetailStatistics(Arrays.asList(rows), true, Tuple2.of(map, new Object[] {1.0, 0.0}),
			Types.DOUBLE);
		assertBinaryMetrics(baseMetric);

		baseMetric = getDetailStatistics(Arrays.asList(rows), "0.000", true, Types.DOUBLE);
		Assert.assertTrue(baseMetric instanceof BinaryMetricsSummary);
		BinaryMetricsSummary metrics = (BinaryMetricsSummary) baseMetric;

		Assert.assertArrayEquals(new Object[] {0.0, 1.0}, metrics.labels);

	}

	@Test
	public void getRankingStatisticsTest() {
		Row[] rows = new Row[] {
			Row.of("{\"object\":\"[1,2,3,4,5]\"}", "{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}"),
			Row.of("{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"),
			Row.of("{\"object\":\"[]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}")
		};
		BaseMetricsSummary baseMetric = getRankingMetrics(
			Arrays.asList(rows), Tuple3.of(10, String.class, 10),
			KObjectUtil.OBJECT_NAME, KObjectUtil.OBJECT_NAME
		);
		RankingMetrics metrics = ((RankingMetricsSummary) baseMetric).merge(null).toMetrics();

		Assert.assertEquals(metrics.getPrecisionAtK(1), 0.66, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(5), 0.33, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(11), 0.24, 0.01);
		Assert.assertEquals(metrics.getPrecisionAtK(15), 0.17, 0.01);
		Assert.assertEquals(metrics.getMap(), 0.30, 0.01);
		Assert.assertEquals(metrics.getNdcg(3), 0.56, 0.01);
		Assert.assertEquals(metrics.getNdcg(5), 0.41, 0.01);
		Assert.assertEquals(metrics.getNdcg(10), 0.43, 0.01);
		Assert.assertEquals(metrics.getNdcg(11), 0.43, 0.01);
		Assert.assertEquals(metrics.getNdcg(12), 0.43, 0.01);
		Assert.assertEquals(metrics.getNdcg(15), 0.43, 0.01);
		Assert.assertEquals(metrics.getHitRate(), 0.33, 0.01);
		Assert.assertEquals(metrics.getArHr(), 0.33, 0.01);
	}

	@Test
	public void getRankingStatisticsTest1() {
		Row[] rows = new Row[] {
			Row.of("{\"object\":\"[0,2]\"}", "{\"object\":\"[0,1]\"}"),
			Row.of("{\"object\":\"[0,1]\"}", "{\"object\":\"[0,2]\"}"),
			Row.of("{\"object\":\"[2]\"}", "{\"object\":\"[2]\"}"),
			Row.of("{\"object\":\"[2,0]\"}", "{\"object\":\"[2,0]\"}"),
			Row.of("{\"object\":\"[0,1]\"}", "{\"object\":\"[0,1,2]\"}"),
			Row.of("{\"object\":\"[1,2]\"}", "{\"object\":\"[1]\"}"),
			Row.of("{\"object\":\"[0]\"}", "{\"object\":\"[]\"}"),
			Row.of("{\"object\":\"[0]\"}", null)
		};
		BaseMetricsSummary baseMetric = getRankingMetrics(
			Arrays.asList(rows), Tuple3.of(3, Integer.class, 3),
			KObjectUtil.OBJECT_NAME, KObjectUtil.OBJECT_NAME
		);
		RankingMetrics metrics = ((RankingMetricsSummary) baseMetric).toMetrics();
		Assert.assertEquals(metrics.getPrecision(), 0.67, 0.01);
		Assert.assertEquals(metrics.getRecall(), 0.64, 0.01);
		Assert.assertEquals(metrics.getAccuracy(), 0.54, 0.01);
		Assert.assertEquals(metrics.getF1(), 0.63, 0.01);
		Assert.assertEquals(metrics.getMicroF1(), 0.69, 0.01);
		Assert.assertEquals(metrics.getMicroPrecision(), 0.72, 0.01);
		Assert.assertEquals(metrics.getMicroRecall(), 0.66, 0.01);
		Assert.assertEquals(metrics.getHammingLoss(), 0.33, 0.01);
		Assert.assertEquals(metrics.getSubsetAccuracy(), 0.28, 0.01);
	}

	private void assertBinaryMetrics(BaseMetricsSummary baseMetric) {
		Assert.assertTrue(baseMetric instanceof BinaryMetricsSummary);
		BinaryMetricsSummary metrics = (BinaryMetricsSummary) baseMetric;
		Assert.assertEquals(5, metrics.total);
		Assert.assertEquals(2.987, metrics.logLoss, 0.01);
		Assert.assertEquals(metrics.positiveBin.length, 100000);
		SparseVector vec = new SparseVector(100000, new int[] {70000, 80000, 90000}, new double[] {1, 1, 1});
		for (int i = 0; i < metrics.positiveBin.length; i++) {
			Assert.assertEquals((int) vec.get(i), metrics.positiveBin[i]);
		}

		Assert.assertEquals(metrics.negativeBin.length, 100000);
		vec = new SparseVector(100000, new int[] {60000, 75000}, new double[] {1, 1});
		for (int i = 0; i < metrics.negativeBin.length; i++) {
			Assert.assertEquals((int) vec.get(i), metrics.negativeBin[i]);
		}
	}

	@Test
	public void getDetailStatisticsMulti() {
		Row[] rows =
			new Row[] {
				Row.of("prefix0", "{\"prefix0\": 0.3, \"prefix1\": 0.2, \"prefix2\": 0.5}"),
				Row.of("prefix0", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.3, \"prefix1\": 0.4, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.5, \"prefix1\": 0.2, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.3, \"prefix2\": 0.3}"),
				Row.of("prefix2", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix2", "{\"prefix0\": 0.6, \"prefix1\": 0.1, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.4, \"prefix1\": 0.1, \"prefix2\": 0.5}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}"),
				Row.of("prefix1", "{\"prefix0\": 0.2, \"prefix1\": 0.5, \"prefix2\": 0.3}")
			};

		BaseMetricsSummary baseMetric = getDetailStatistics(Arrays.asList(rows), null, false, Types.STRING);
		assertMultiMetrics(baseMetric);

		Map <Object, Integer> map = new HashMap <>();
		map.put("prefix0", 2);
		map.put("prefix1", 1);
		map.put("prefix2", 0);

		baseMetric = getDetailStatistics(Arrays.asList(rows), false,
			Tuple2.of(map, new Object[] {"prefix2", "prefix1", "prefix0"}), Types.STRING);
		assertMultiMetrics(baseMetric);
	}

	private void assertMultiMetrics(BaseMetricsSummary baseMetric) {
		Assert.assertTrue(baseMetric instanceof MultiMetricsSummary);
		MultiMetricsSummary metrics = (MultiMetricsSummary) baseMetric;

		Assert.assertEquals(10, metrics.total);
		Assert.assertEquals(11.03, metrics.logLoss, 0.01);
		Assert.assertArrayEquals(new Object[] {"prefix2", "prefix1", "prefix0"}, metrics.labels);

		long[][] matrix = metrics.matrix.getMatrix();
		Assert.assertArrayEquals(new long[] {1, 1, 1}, matrix[0]);
		Assert.assertArrayEquals(new long[] {0, 3, 0}, matrix[1]);
		Assert.assertArrayEquals(new long[] {3, 0, 1}, matrix[2]);
	}

	@Test
	public void predResultMatrixTest() {
		Row[] rows =
			new Row[] {
				Row.of("prefix0", "prefix2"),
				Row.of("prefix0", "prefix0"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix2", "prefix2"),
				Row.of("prefix2", "prefix0"),
				Row.of("prefix1", "prefix2"),
				Row.of("prefix1", "prefix1"),
				Row.of("prefix1", "prefix1")
			};

		Map <Object, Integer> map = new HashMap <>();
		map.put("prefix0", 2);
		map.put("prefix1", 1);
		map.put("prefix2", 0);

		MultiMetricsSummary metrics = EvaluationUtil.getMultiClassMetrics(Arrays.asList(rows),
			Tuple2.of(map, new Object[] {"prefix2", "prefix1", "prefix0"}));

		Assert.assertEquals(10, metrics.total);
		Assert.assertArrayEquals(new Object[] {"prefix2", "prefix1", "prefix0"}, metrics.labels);
		Assert.assertArrayEquals(new long[] {1, 1, 1}, metrics.matrix.getMatrix()[0]);
		Assert.assertArrayEquals(new long[] {0, 3, 0}, metrics.matrix.getMatrix()[1]);
		Assert.assertArrayEquals(new long[] {3, 0, 1}, metrics.matrix.getMatrix()[2]);
	}

	@Test
	public void testZeroEffitiveData() {
		Row[] rows =
			new Row[] {
				Row.of("prefix0", null)
			};

		Map <Object, Integer> map = new HashMap <>();
		map.put("prefix0", 2);
		map.put("prefix1", 1);
		map.put("prefix2", 0);

		MultiMetricsSummary metrics = EvaluationUtil.getMultiClassMetrics(Arrays.asList(rows),
			Tuple2.of(map, new Object[] {"prefix2", "prefix1", "prefix0"}));
		Assert.assertNull(metrics);
	}

	@Test
	public void testCastTo() {
		Assert.assertTrue(EvaluationUtil.castTo("true", Types.BOOLEAN) instanceof Boolean);
		Assert.assertTrue(EvaluationUtil.castTo(true, Types.BOOLEAN) instanceof Boolean);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.SHORT) instanceof Short);
		Assert.assertTrue(EvaluationUtil.castTo((short) 1, Types.SHORT) instanceof Short);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.INT) instanceof Integer);
		Assert.assertTrue(EvaluationUtil.castTo(1, Types.INT) instanceof Integer);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.LONG) instanceof Long);
		Assert.assertTrue(EvaluationUtil.castTo(1L, Types.LONG) instanceof Long);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.BYTE) instanceof Byte);
		Assert.assertTrue(EvaluationUtil.castTo((byte) 1, Types.BYTE) instanceof Byte);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.FLOAT) instanceof Float);
		Assert.assertTrue(EvaluationUtil.castTo((float) 1, Types.FLOAT) instanceof Float);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.DOUBLE) instanceof Double);
		Assert.assertTrue(EvaluationUtil.castTo((double) 1, Types.DOUBLE) instanceof Double);
		Assert.assertTrue(EvaluationUtil.castTo("1", Types.STRING) instanceof String);

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("unsupported type: org.apache.flink.api.common.typeinfo.BasicTypeInfo");
		EvaluationUtil.castTo("1", Types.BIG_DEC);
	}

	@Test
	public void testCompare() {
		Assert.assertEquals(EvaluationUtil.compare(1L, 2L), -1);
		Assert.assertEquals(EvaluationUtil.compare(1L, null), 1);
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Input Labels are not comparable!");
		EvaluationUtil.compare(1L, 0.1);
	}

	@Test
	public void testException1() {
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Failed to deserialize prediction detail: a, b, c.");
		EvaluationUtil.extractLabelProbMap(Row.of(null, "a, b, c"), Types.LONG);
	}

}