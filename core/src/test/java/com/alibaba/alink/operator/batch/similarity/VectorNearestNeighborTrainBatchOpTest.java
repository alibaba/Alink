package com.alibaba.alink.operator.batch.similarity;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.recommendation.KObjectUtil;
import com.alibaba.alink.params.shared.clustering.HasFastMetric;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorNearestNeighborTrainBatchOpTest extends AlinkTestBase {
	static Row[] dictRows = new Row[] {
		Row.of("dict1", "0 0 0"),
		Row.of("dict2", "0.1 0.1 0.1"),
		Row.of("dict3", "0.2 0.2 0.2"),
		Row.of("dict4", "9 9 9"),
		Row.of("dict5", "9.1 9.1 9.1"),
		Row.of("dict6", "9.2 9.2 9.2")
	};
	static Row[] queryRows = new Row[] {
		Row.of(1, "0 0 0"),
		Row.of(2, "0.1 0.1 0.1"),
		Row.of(3, "0.2 0.2 0.2"),
		Row.of(4, "9 9 9"),
		Row.of(5, "9.1 9.1 9.1"),
		Row.of(6, "9.2 9.2 9.2")
	};

	public static Double[] extractScore(String res) {
		List <Object> list = KObjectUtil.deserializeKObject(
			res, new String[] {"METRIC"}, new Type[] {Double.class}
		).get("METRIC");
		if (null != list) {
			return list.toArray(new Double[0]);
		} else {
			return new Double[0];
		}
	}

	@Test
	public void testEuclidean() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(dictRows), new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(Arrays.asList(queryRows), new String[] {"id", "vec"});

		VectorNearestNeighborTrainBatchOp train = new VectorNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setMetric(HasFastMetric.Metric.EUCLIDEAN)
			.linkFrom(dict);

		VectorNearestNeighborPredictBatchOp predict = new VectorNearestNeighborPredictBatchOp()
			.setNumThreads(4)
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
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
		System.out.println(this.getClass().getSimpleName() + "," + this.getClass().getCanonicalName());
	}

	@Test
	public void testJaccard() throws Exception {
		BatchOperator dict = new MemSourceBatchOp(Arrays.asList(VectorApproxNearestNeighborTrainBatchOpTest.dictRows),
			new String[] {"id", "vec"});
		BatchOperator query = new MemSourceBatchOp(
			Arrays.asList(VectorApproxNearestNeighborTrainBatchOpTest.queryRows), new String[] {"id", "vec"});

		VectorNearestNeighborTrainBatchOp train = new VectorNearestNeighborTrainBatchOp()
			.setIdCol("id")
			.setSelectedCol("vec")
			.setMetric(HasFastMetric.Metric.JACCARD)
			.linkFrom(dict);

		VectorNearestNeighborPredictBatchOp predict = new VectorNearestNeighborPredictBatchOp()
			.setSelectedCol("vec")
			.setOutputCol("topN")
			.setTopN(3)
			.linkFrom(train, query);

		List <Row> res = predict.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(1, new Double[] {0.6666666666666667, 0.6666666666666667, 1.0});
		score.put(2, new Double[] {0.0, 0.6666666666666667, 1.0});
		score.put(3, new Double[] {0.6666666666666667, 0.6666666666666667, 1.0});
		score.put(4, new Double[] {0.0, 0.6666666666666667, 1.0});
		score.put(5, new Double[] {0.0, 0.6666666666666667, 1.0});
		score.put(6, new Double[] {0.6666666666666667, 0.6666666666666667, 0.6666666666666667});

		for (Row row : res) {
			Double[] actual = extractScore((String) row.getField(2));
			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}
}