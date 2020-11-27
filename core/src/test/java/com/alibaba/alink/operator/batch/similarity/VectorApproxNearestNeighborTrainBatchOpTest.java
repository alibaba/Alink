package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.similarity.Solver;
import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.similarity.VectorApproxNearestNeighborTrainParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorApproxNearestNeighborTrainBatchOpTest extends AlinkTestBase {
	static Row[] dictRows = new Row[] {
		Row.of("dict1", "$10$"),
		Row.of("dict2", "$10$1:1.0 5:2.0"),
		Row.of("dict3", "$10$4:1.0 6:2.0"),
		Row.of("dict4", "$10$2:1.0 7:2.0"),
		Row.of("dict5", "$10$3:1.0 5:2.0"),
		Row.of("dict6", "$10$4:1.0 7:2.0")
	};
	static Row[] queryRows = new Row[] {
		Row.of(1, "$10$1:1.0 2:2.0"),
		Row.of(2, "$10$2:1.0 7:2.0"),
		Row.of(3, "$10$2:1.0 3:2.0"),
		Row.of(4, "$10$4:1.0 6:2.0"),
		Row.of(5, "$10$3:1.0 5:2.0"),
		Row.of(6, "$10$5:1.0 7:2.0")
	};

	@Test
	public void testKDTree() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(VectorNearestNeighborTrainBatchOpTest.dictRows),
			new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(VectorNearestNeighborTrainBatchOpTest.queryRows),
			new String[] {"id", "vec"});

		VectorApproxNearestNeighborTrainBatchOp train = new VectorApproxNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setMetric(VectorApproxNearestNeighborTrainParams.Metric.EUCLIDEAN)
			.setSolver(Solver.KDTREE)
			.linkFrom(dict);

		VectorApproxNearestNeighborPredictBatchOp predict = new VectorApproxNearestNeighborPredictBatchOp()
			.setSelectedCol("vec")
			.setOutputCol("topN")
			.setTopN(3)
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(2, new Double[] {0.0, 0.17320508075688773, 0.17320508075688776});
		score.put(3, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(4, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});
		score.put(5, new Double[] {0.0, 0.17320508075680896, 0.17320508075680896});
		score.put(6, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});

		for (Row row : res) {
			Double[] actual = VectorNearestNeighborTrainBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testLSH() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(VectorNearestNeighborTrainBatchOpTest.dictRows),
			new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(VectorNearestNeighborTrainBatchOpTest.queryRows),
			new String[] {"id", "vec"});

		VectorApproxNearestNeighborTrainBatchOp train = new VectorApproxNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setMetric(VectorApproxNearestNeighborTrainParams.Metric.EUCLIDEAN)
			.setSolver(Solver.LSH)
			.linkFrom(dict);

		VectorApproxNearestNeighborPredictBatchOp predict = new VectorApproxNearestNeighborPredictBatchOp()
			.setSelectedCol("vec")
			.setOutputCol("topN")
			.setTopN(3)
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(2, new Double[] {0.0, 0.17320508075688776, 0.17320508075688773});
		score.put(3, new Double[] {0.0, 0.17320508075688776, 0.3464101615137755});
		score.put(4, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});
		score.put(5, new Double[] {0.0, 0.17320508075680896, 0.17320508075680896});
		score.put(6, new Double[] {0.0, 0.17320508075680896, 0.346410161513782});

		for (Row row : res) {
			Double[] actual = VectorNearestNeighborTrainBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testJaccard() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(dictRows), new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(queryRows), new String[] {"id", "vec"});

		VectorApproxNearestNeighborTrainBatchOp train = new VectorApproxNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setMetric(VectorApproxNearestNeighborTrainParams.Metric.JACCARD)
			.setSolver(Solver.LSH)
			.linkFrom(dict);

		VectorApproxNearestNeighborPredictBatchOp predict = new VectorApproxNearestNeighborPredictBatchOp(
			new Params().set(HasNumThreads.NUM_THREADS, 4))
			.setSelectedCol("vec")
			.setOutputCol("topN")
			.setTopN(3)
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.6666666666666667});
		score.put(2, new Double[] {0.0});
		score.put(3, new Double[] {});
		score.put(4, new Double[] {0.0});
		score.put(5, new Double[] {0.0, 0.6666666666666667});
		score.put(6, new Double[] {0.6666666666666667, 0.6666666666666667});

		for (Row row : res) {
			Double[] actual = VectorNearestNeighborTrainBatchOpTest.extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}
}