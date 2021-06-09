package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.operator.common.recommendation.ItemCfRecommKernelTest;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for UserCfRecommTrain.
 */
public class UserCfTrainBatchOpTest extends AlinkTestBase {
	private Row[] rows = new Row[] {
		Row.of(1L, "item1", 4.0),
		Row.of(1L, "item2", 3.0),
		Row.of(1L, "item5", 5.0),
		Row.of(2L, "item1", 5.0),
		Row.of(2L, "item3", 4.0),
		Row.of(2L, "item5", 4.0),
		Row.of(3L, "item1", 4.0),
		Row.of(3L, "item3", 5.0),
		Row.of(3L, "item4", 3.0),
		Row.of(3L, "item5", 4.0),
		Row.of(4L, "item2", 3.0),
		Row.of(4L, "item6", 5.0),
		Row.of(5L, "item2", 4.0),
		Row.of(5L, "item6", 4.0),
		Row.of(6L, "item3", 2.0),
		Row.of(6L, "item4", 4.0),
		Row.of(6L, "item6", 5.0)
	};

	@Test
	public void test() throws Exception {
		BatchOperator <?> data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"user", "item", "rating"}));

		UserCfTrainBatchOp op = new UserCfTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setSimilarityType("COSINE")
			.setRateCol("rating")
			.linkFrom(data);

		List <Row> similarUsers = new UserCfSimilarUsersRecommBatchOp()
			.setUserCol("user")
			.setK(3)
			.setRecommCol("recomm")
			.linkFrom(op, data)
			.collect();

		List <Row> items = new UserCfItemsPerUserRecommBatchOp()
			.setUserCol("user")
			.setRecommCol("recomm")
			.linkFrom(op, data)
			.collect();

		List <Row> recommendUsers = new UserCfUsersPerItemRecommBatchOp()
			.setItemCol("item")
			.setK(3)
			.setRecommCol("recomm")
			.linkFrom(op, data)
			.collect();

		BatchOperator rate = new UserCfRateRecommBatchOp()
			.setItemCol("item")
			.setUserCol("user")
			.setRecommCol("recomm")
			.linkFrom(op, data);

		RegressionMetrics metrics = new EvalRegressionBatchOp()
			.setLabelCol("rating")
			.setPredictionCol("recomm")
			.linkFrom(rate)
			.collectMetrics();

		Assert.assertEquals(metrics.getRmse(), 1.05, 0.01);

		Map <Object, Double[]> score = new HashMap <>();
		score.put("item1", new Double[] {2.35, 2.21, 2.08});
		score.put("item2", new Double[] {1.51, 1.34, 1.27});
		score.put("item3", new Double[] {2.04, 1.62, 1.48});
		score.put("item4", new Double[] {1.68, 1.27, 1.05});
		score.put("item5", new Double[] {2.46, 2.26, 1.83});
		score.put("item6", new Double[] {2.49, 2.35, 1.76});

		for (Row row : recommendUsers) {
			Double[] actual = ItemCfRecommKernelTest.extractScore((String) row.getField(3), "score");
			Double[] expect = score.get(row.getField(1));
			for (int i = 0; i < expect.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}

		Map <Object, Double[]> similarity = new HashMap <>();
		similarity.put(1L, new Double[] {0.74, 0.62, 0.30});
		similarity.put(2L, new Double[] {0.91, 0.74, 0.15});
		similarity.put(3L, new Double[] {0.91, 0.62, 0.40});
		similarity.put(4L, new Double[] {0.97, 0.63, 0.21});
		similarity.put(5L, new Double[] {0.97, 0.52, 0.30});
		similarity.put(6L, new Double[] {0.63, 0.52, 0.40});

		for (Row row : similarUsers) {
			Double[] actual = ItemCfRecommKernelTest.extractScore((String) row.getField(3), "similarities");
			Double[] expect = similarity.get(row.getField(0));
			for (int i = 0; i < expect.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testLazyPrint() throws Exception {
		BatchOperator data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"user", "item", "rating"}));

		UserCfTrainBatchOp op = new UserCfTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setSimilarityType("COSINE")
			.setRateCol("rating")
			.linkFrom(data);

		op.lazyPrintModelInfo();

		BatchOperator.execute();
	}
}