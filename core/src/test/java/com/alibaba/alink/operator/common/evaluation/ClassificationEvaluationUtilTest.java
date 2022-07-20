package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.BinaryPartitionSummary;
import com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.CalcBinaryPartitionSummary;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.common.evaluation.ClassificationEvaluationUtil.DECISION_THRESHOLD_BC_NAME;
import static com.alibaba.alink.operator.common.evaluation.EvaluationUtil.getDetailStatistics;

/**
 * Unit test for ClassificationEvaluationUtil.
 */

public class ClassificationEvaluationUtilTest extends AlinkTestBase {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void judgeEvaluationTypeTest() {
		Params params = new Params()
			.set(HasPredictionDetailCol.PREDICTION_DETAIL_COL, "detail");

		ClassificationEvaluationUtil.Type type = ClassificationEvaluationUtil.judgeEvaluationType(params);
		Assert.assertEquals(type, ClassificationEvaluationUtil.Type.PRED_DETAIL);

		params.set(HasPredictionCol.PREDICTION_COL, "pred");
		type = ClassificationEvaluationUtil.judgeEvaluationType(params);
		Assert.assertEquals(type, ClassificationEvaluationUtil.Type.PRED_DETAIL);

		params.remove(HasPredictionDetailCol.PREDICTION_DETAIL_COL);
		type = ClassificationEvaluationUtil.judgeEvaluationType(params);
		Assert.assertEquals(type, ClassificationEvaluationUtil.Type.PRED_RESULT);

		params.remove(HasPredictionCol.PREDICTION_COL);
		thrown.expect(RuntimeException.class);
		thrown.expectMessage("Error Input, must give either predictionCol or predictionDetailCol!");
		ClassificationEvaluationUtil.judgeEvaluationType(params);
	}

	@Test
	public void predResultLabelMapExceptionWhenBinaryClassification() {
		HashSet <Object> set = new HashSet <>();
		set.add("0");
		thrown.expect(AkIllegalArgumentException.class);
		thrown.expectMessage("The number of labels must be equal to 2!");
		ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, true, null, Types.INT, true);
	}

	@Test
	public void predResultLabelMapWhenMultiClassification() {
		HashSet <Object> set = new HashSet <>();
		set.add("0");
		Tuple2 <Map <Object, Integer>, Object[]> labelIndexLabelArray =
			ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, false, null, Types.INT, true);
		Assert.assertEquals(1, labelIndexLabelArray.f0.size());
		Assert.assertArrayEquals(new Object[] {"0"}, labelIndexLabelArray.f1);
	}

	@Test
	public void predResultLabelMapTest() {
		HashSet <Object> set = new HashSet <>();
		set.add("1.0");
		set.add("0.0");
		Tuple2 <Map <Object, Integer>, Object[]> tuple2 = ClassificationEvaluationUtil.buildLabelIndexLabelArray(set,
			true, null, Types.DOUBLE, true);
		Map <Object, Integer> map = tuple2.f0;
		Assert.assertEquals(map.size(), 2);
		Assert.assertEquals((int) map.get("0.0"), 1);
		Assert.assertEquals((int) map.get("1.0"), 0);

		tuple2 = ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, true, "0", Types.DOUBLE, true);
		map = tuple2.f0;
		Assert.assertEquals(map.size(), 2);
		Assert.assertEquals((int) map.get("0.0"), 0);
		Assert.assertEquals((int) map.get("1.0"), 1);

		set.add("2.0");
		tuple2 = ClassificationEvaluationUtil.buildLabelIndexLabelArray(set, false, null, Types.DOUBLE, true);
		map = tuple2.f0;
		Assert.assertEquals(map.size(), 3);
		Assert.assertEquals((int) map.get("0.0"), 2);
		Assert.assertEquals((int) map.get("1.0"), 1);
		Assert.assertEquals((int) map.get("2.0"), 0);

		Assert.assertArrayEquals(new String[] {"2.0", "1.0", "0.0"}, tuple2.f1);
	}

	@Test
	public void getDetailBinary() {
		Row[] rows =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"),
				Row.of(null, "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
			};

		Object[] labels = new Object[] {"prefix1", "prefix0"};
		TypeInformation labelType = Types.STRING;
		Tuple3 <Double, Boolean, Double> t = ClassificationEvaluationUtil.getBinaryDetailStatistics(rows[0], labels,
			labelType);
		Assert.assertEquals(t.f0, 0.9, 0.01);
		Assert.assertTrue(t.f1);

		t = ClassificationEvaluationUtil.getBinaryDetailStatistics(rows[4], labels, labelType);
		Assert.assertEquals(t.f0, 0.6, 0.01);
		Assert.assertFalse(t.f1);

		rows =
			new Row[] {
				Row.of(1.0, "{\"1.00\": 0.9, \"0.00\": 0.1}"),
				Row.of(1.00, "{\"1.0\": 0.8, \"0.00\": 0.2}"),
				Row.of(1.0, "{\"1.00\": 0.7, \"0.000\": 0.3}"),
				Row.of(0.0, "{\"1.0\": 0.75, \"0.0\": 0.25}"),
				Row.of(null, "{\"1.0\": 0.75, \"0.0\": 0.25}"),
				Row.of(0.00, "{\"1.0\": 0.6, \"0.00\": 0.4}")
			};
		labels = new Object[] {1.0, 0.0};
		labelType = Types.DOUBLE;
		t = ClassificationEvaluationUtil.getBinaryDetailStatistics(rows[1], labels, labelType);
		Assert.assertEquals(t.f0, 0.8, 0.01);
		Assert.assertTrue(t.f1);

		t = ClassificationEvaluationUtil.getBinaryDetailStatistics(rows[5], labels, labelType);
		Assert.assertEquals(t.f0, 0.6, 0.01);
		Assert.assertFalse(t.f1);
	}

	@Test
	public void updateBinaryPartitionSummaryTest() {
		Row[] rows =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"),
				Row.of(null, "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
			};

		Object[] labels = new Object[] {"prefix1", "prefix0"};
		TypeInformation labelType = Types.STRING;
		ClassificationEvaluationUtil.BinaryPartitionSummary summary = new ClassificationEvaluationUtil
			.BinaryPartitionSummary(0, 0, 0, 0);
		for (Row row : rows) {
			Tuple3 <Double, Boolean, Double> t = ClassificationEvaluationUtil.getBinaryDetailStatistics(row, labels,
				labelType);
			if (null != t) {
				ClassificationEvaluationUtil.updateBinaryPartitionSummary(summary, t, 0.5);
			}
		}
		Assert.assertEquals(summary.curNegative, 2);
		Assert.assertEquals(summary.curPositive, 3);
		Assert.assertEquals((int) summary.taskId, 0);
		Assert.assertEquals(summary.maxScore, 0.9, 0.01);
	}

	@Test
	public void reduceBinaryPartitionSummaryTest() {
		List <ClassificationEvaluationUtil.BinaryPartitionSummary> list = new ArrayList <>();
		ClassificationEvaluationUtil.BinaryPartitionSummary summary1 = new ClassificationEvaluationUtil
			.BinaryPartitionSummary(0, 0.9, 5, 3);
		list.add(summary1);
		ClassificationEvaluationUtil.BinaryPartitionSummary summary2 = new ClassificationEvaluationUtil
			.BinaryPartitionSummary(1, 0.7, 4, 1);
		list.add(summary2);
		ClassificationEvaluationUtil.BinaryPartitionSummary summary3 = new ClassificationEvaluationUtil
			.BinaryPartitionSummary(2, 0.4, 3, 6);
		list.add(summary3);
		Tuple2 <Boolean, long[]> t = ClassificationEvaluationUtil.reduceBinaryPartitionSummary(list, 0);
		Assert.assertTrue(t.f0);
		Assert.assertArrayEquals(t.f1, new long[] {0, 0, 12, 10});
		t = ClassificationEvaluationUtil.reduceBinaryPartitionSummary(list, 2);
		Assert.assertFalse(t.f0);
		Assert.assertArrayEquals(t.f1, new long[] {9, 4, 12, 10});
	}

	@Test
	public void updateAccurateBinaryMetricsSummaryTest() {
		long[] countValues = new long[] {0, 0, 3, 2};
		Row[] rows =
			new Row[] {
				Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
				Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
				Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
				Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
				Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}")
			};

		Object[] labels = new Object[] {"prefix1", "prefix0"};
		TypeInformation labelType = Types.STRING;
		AccurateBinaryMetricsSummary summary = new AccurateBinaryMetricsSummary(labels, 0.0, 0L, 0.);
		double[] tprFprPrecision = new double[ClassificationEvaluationUtil.RECORD_LEN];
		List <Tuple3 <Double, Boolean, Double>> list = new ArrayList <>();
		for (Row row : rows) {
			list.add(ClassificationEvaluationUtil.getBinaryDetailStatistics(row, labels,
				labelType));
		}
		Collections.sort(list, Comparator.comparingDouble(t -> -t.f0));
		list.forEach(t -> ClassificationEvaluationUtil.updateAccurateBinaryMetricsSummary(t, summary, countValues,
			tprFprPrecision, true, 0.5, 1.0));

		BinaryClassMetrics summary1 = (BinaryClassMetrics) getDetailStatistics(Arrays.asList(rows), null, true,
			Types.STRING).toMetrics();
		BinaryClassMetrics summary2 = summary.toMetrics();
		Assert.assertEquals(summary1.getTotalSamples(), summary2.getTotalSamples());
		//Assert.assertEquals(summary1.getAuc(), summary2.getAuc());
		Assert.assertEquals(summary1.getKs(), summary2.getKs());
		Assert.assertEquals(summary1.getPrc(), summary2.getPrc());
		Assert.assertEquals(summary1.getGini(), summary2.getGini());
	}

	BinaryPartitionSummary runCalcBinaryPartitionSummary(
		RuntimeContext runtimeContext, List <Tuple3 <Double, Boolean, Double>> values) {
		CalcBinaryPartitionSummary f = new CalcBinaryPartitionSummary();
		f.setRuntimeContext(runtimeContext);
		f.open(null);
		List <BinaryPartitionSummary> results = new ArrayList <>();
		f.mapPartition(values, new ListCollector <>(results));
		Assert.assertEquals(1, results.size());
		return results.get(0);
	}

	@Test
	public void testCalcBinaryPartitionSummaryProbs() {
		//noinspection unchecked
		Tuple3 <Double, Boolean, Double>[] rows = new Tuple3[] {
			Tuple3.of(0.9, true, 0.1),
			Tuple3.of(0.8, true, 0.1),
			Tuple3.of(0.7, true, 0.1),
			Tuple3.of(0.75, false, 0.1),
			Tuple3.of(0.6, false, 0.1),
		};
		List <Tuple3 <Double, Boolean, Double>> values = Arrays.asList(rows);

		RuntimeContext mockRuntimeContext = Mockito.mock(RuntimeContext.class);
		int taskId = 2;
		Mockito.when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(taskId);

		BinaryPartitionSummary summary = runCalcBinaryPartitionSummary(mockRuntimeContext, values);
		Assert.assertEquals(summary.curNegative, 2);
		Assert.assertEquals(summary.curPositive, 3);
		Assert.assertEquals((int) summary.taskId, taskId);
		Assert.assertEquals(summary.maxScore, 0.9, 0.01);
	}

	@Test
	public void testCalcBinaryPartitionSummaryProbsWithDecisionThresh() {
		//noinspection unchecked
		Tuple3 <Double, Boolean, Double>[] rows = new Tuple3[] {
			Tuple3.of(0.9, true, 0.1),
			Tuple3.of(0.8, true, 0.1),
			Tuple3.of(0.7, true, Double.NaN),
			Tuple3.of(0.75, false, 0.1),
			Tuple3.of(0.6, false, 0.1),
		};
		List <Tuple3 <Double, Boolean, Double>> values = Arrays.asList(rows);

		RuntimeContext mockRuntimeContext = Mockito.mock(RuntimeContext.class);
		int taskId = 2;
		Mockito.when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(taskId);
		Mockito.when(mockRuntimeContext.hasBroadcastVariable(DECISION_THRESHOLD_BC_NAME))
			.thenReturn(true);
		Mockito.when(mockRuntimeContext.getBroadcastVariable(DECISION_THRESHOLD_BC_NAME))
			.thenReturn(Collections.singletonList(0.7));

		BinaryPartitionSummary summary = runCalcBinaryPartitionSummary(mockRuntimeContext, values);
		Assert.assertEquals(summary.curNegative, 2);
		Assert.assertEquals(summary.curPositive, 2);
		Assert.assertEquals((int) summary.taskId, taskId);
		Assert.assertEquals(summary.maxScore, 0.9, 0.01);
	}

	@Test
	public void testCalcBinaryPartitionSummaryNegScores() {
		//noinspection unchecked
		Tuple3 <Double, Boolean, Double>[] rows = new Tuple3[] {
			Tuple3.of(-0.9, true, 0.1),
			Tuple3.of(-0.8, true, 0.1),
			Tuple3.of(-0.7, true, 0.1),
			Tuple3.of(-0.75, false, 0.1),
			Tuple3.of(-0.6, false, 0.1),
		};
		List <Tuple3 <Double, Boolean, Double>> values = Arrays.asList(rows);

		RuntimeContext mockRuntimeContext = Mockito.mock(RuntimeContext.class);
		int taskId = 2;
		Mockito.when(mockRuntimeContext.getIndexOfThisSubtask()).thenReturn(taskId);

		BinaryPartitionSummary summary = runCalcBinaryPartitionSummary(mockRuntimeContext, values);
		Assert.assertEquals(summary.curNegative, 2);
		Assert.assertEquals(summary.curPositive, 3);
		Assert.assertEquals((int) summary.taskId, taskId);
		Assert.assertEquals(summary.maxScore, -0.6, 0.01);
	}
}
