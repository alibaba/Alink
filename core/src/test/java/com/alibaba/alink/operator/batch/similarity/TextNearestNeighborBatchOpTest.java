package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.similarity.StringTextNearestNeighborTrainParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.alink.operator.batch.similarity.StringNearestNeighborBatchOpTest.extractScore;

/**
 * Test for ApproxTextSimilarityTopNBatchOp.
 */
public class TextNearestNeighborBatchOpTest extends AlinkTestBase {

	@Test
	public void testLevenshteinSim() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		TextNearestNeighborTrainBatchOp train = new TextNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("str")
			.setMetric(StringTextNearestNeighborTrainParams.Metric.LEVENSHTEIN_SIM)
			.linkFrom(dict);

		TextNearestNeighborPredictBatchOp predict = new TextNearestNeighborPredictBatchOp(
			new Params().set(HasNumThreads.NUM_THREADS, 4))
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("topN")
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.75, 0.667, 0.333});
		score.put(2, new Double[] {0.667, 0.667, 0.5});
		score.put(3, new Double[] {0.333, 0.333, 0.25});
		score.put(4, new Double[] {0.75, 0.333, 0.333});
		score.put(5, new Double[] {0.333, 0.25, 0.25});
		score.put(6, new Double[] {0.333, 0.333, 0.333});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testLevenshtein() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		TextNearestNeighborTrainBatchOp train = new TextNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("str")
			.setMetric(StringTextNearestNeighborTrainParams.Metric.LEVENSHTEIN)
			.linkFrom(dict);

		TextNearestNeighborPredictBatchOp predict = new TextNearestNeighborPredictBatchOp()
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("topN")
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {1.0, 1.0, 2.0});
		score.put(2, new Double[] {1.0, 1.0, 2.0});
		score.put(3, new Double[] {2.0, 3.0, 3.0});
		score.put(4, new Double[] {1.0, 2.0, 3.0});
		score.put(5, new Double[] {3.0, 3.0, 3.0});
		score.put(6, new Double[] {2.0, 2.0, 2.0});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testLCSSIM() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		TextNearestNeighborTrainBatchOp train = new TextNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("str")
			.setMetric(StringTextNearestNeighborTrainParams.Metric.LCS_SIM)
			.linkFrom(dict);

		TextNearestNeighborPredictBatchOp predict = new TextNearestNeighborPredictBatchOp()
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("topN")
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.75, 0.667, 0.667});
		score.put(2, new Double[] {0.667, 0.667, 0.5});
		score.put(3, new Double[] {0.333, 0.333, 0.333});
		score.put(4, new Double[] {0.75, 0.333, 0.333});
		score.put(5, new Double[] {0.333, 0.333, 0.25});
		score.put(6, new Double[] {0.5, 0.333, 0.333});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testLCS() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		TextNearestNeighborTrainBatchOp train = new TextNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("str")
			.setMetric(StringTextNearestNeighborTrainParams.Metric.LCS)
			.linkFrom(dict);

		TextNearestNeighborPredictBatchOp predict = new TextNearestNeighborPredictBatchOp()
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("topN")
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {3.0, 2.0, 2.0});
		score.put(2, new Double[] {3.0, 2.0, 2.0});
		score.put(3, new Double[] {2.0, 1.0, 1.0});
		score.put(4, new Double[] {3.0, 2.0, 1.0});
		score.put(5, new Double[] {2.0, 1.0, 1.0});
		score.put(6, new Double[] {2.0, 2.0, 1.0});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testSSK() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		TextNearestNeighborTrainBatchOp train = new TextNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("str")
			.setMetric(StringTextNearestNeighborTrainParams.Metric.SSK)
			.linkFrom(dict);

		TextNearestNeighborPredictBatchOp predict = new TextNearestNeighborPredictBatchOp()
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("topN")
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.794, 0.667, 0.222});
		score.put(2, new Double[] {0.444, 0.353, 0.35});
		score.put(3, new Double[] {0.33, 0.0, 0.0});
		score.put(4, new Double[] {0.794, 0.06, 0.0});
		score.put(5, new Double[] {0.33, 0.0, 0.0});
		score.put(6, new Double[] {0.133, 0.08, 0.0});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testCosine() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.dictRows),
			new String[] {"id", "str"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(TextApproxNearestNeighborBatchOpTest.queryRows),
			new String[] {"id", "str"});

		TextNearestNeighborTrainBatchOp train = new TextNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("str")
			.setMetric(StringTextNearestNeighborTrainParams.Metric.COSINE)
			.linkFrom(dict);

		TextNearestNeighborPredictBatchOp predict = new TextNearestNeighborPredictBatchOp()
			.setSelectedCol("str")
			.setTopN(3)
			.setOutputCol("topN")
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.816, 0.707, 0.0});
		score.put(2, new Double[] {0.5, 0.316, 0.0});
		score.put(3, new Double[] {0.316, 0.0, 0.0});
		score.put(4, new Double[] {0.816, 0.0, 0.0});
		score.put(5, new Double[] {0.316, 0.0, 0.0});
		score.put(6, new Double[] {0.0, 0.0, 0.0});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

}