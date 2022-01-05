package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.operator.common.recommendation.Zipped2KObjectBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.PipelineModel;
import com.alibaba.alink.pipeline.recommendation.ItemCfItemsPerUserRecommender;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for ItemSimilarityRecommTrainBatchOp.
 */

public class ItemCfTrainBatchOpTest extends AlinkTestBase {
	private final Row[] rows1 = new Row[] {
		Row.of(1L, 1L, 0.7),
		Row.of(1L, 2L, 0.1),
		Row.of(1L, 3L, 0.6),
		Row.of(1L, 1L, 0.5),
		Row.of(2L, 2L, 0.8),
		Row.of(2L, 3L, 0.6),
		Row.of(2L, 1L, 0.0),
		Row.of(2L, 2L, 0.7),
		Row.of(2L, 3L, 0.4),
		Row.of(3L, 1L, 0.6),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.4),
		Row.of(3L, 1L, 0.9),
		Row.of(3L, 2L, 0.3),
		Row.of(3L, 3L, 0.1),
	};

	private final Row[] rows = new Row[] {
		Row.of(0L, "a", 1.0),
		Row.of(0L, "b", 3.0),
		Row.of(0L, "c", 2.0),
		Row.of(1L, "a", 5.0),
		Row.of(1L, "b", 4.0),
		Row.of(2L, "b", 1.0),
		Row.of(2L, "c", 4.0),
		Row.of(2L, "d", 3.0)
	};

	@Test
	public void testCosine() {
		BatchOperator<?> emptyRate = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"user", "item", "rate"}));

		ItemCfTrainBatchOp trainBatchOp = new ItemCfTrainBatchOp()
			.setSimilarityType("COSINE")
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rate")
			.linkFrom(emptyRate);

		Zipped2KObjectBatchOp zipUser = new Zipped2KObjectBatchOp()
			.setGroupCol("user")
			.setObjectCol("item")
			.setInfoCols("rate")
			.setOutputCol("label")
			.linkFrom(emptyRate);

		Zipped2KObjectBatchOp zipItem = new Zipped2KObjectBatchOp()
			.setGroupCol("item")
			.setObjectCol("user")
			.setInfoCols("rate")
			.setOutputCol("label")
			.linkFrom(emptyRate).lazyPrint(-1);

		ItemCfRateRecommBatchOp rateRecommBatchOp = new ItemCfRateRecommBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRecommCol("recommend")
			.setNumThreads(4)
			.linkFrom(trainBatchOp, emptyRate);

		RegressionMetrics metrics = new EvalRegressionBatchOp()
			.setLabelCol("rate")
			.setPredictionCol("recommend")
			.linkFrom(rateRecommBatchOp)
			.collectMetrics();
		Assert.assertEquals(metrics.getRmse(), 1.544, 0.01);
		Assert.assertEquals(metrics.getMae(), 1.381, 0.01);

		List <Row> recommendItems = new ItemCfItemsPerUserRecommBatchOp()
			.setUserCol("user")
			.setRecommCol("recomm")
			.linkFrom(trainBatchOp, zipUser)
			.collect();

		Map <Object, Double[]> score = new HashMap <>();
		score.put(0L, new Double[] {0.9430859192358533, 0.792400929138128, 0.587224467974138, 0.4677642769637489});
		score.put(1L, new Double[] {2.211538461538462, 1.7692307692307696, 1.0963225241337864, 0.3922322702763681});
		score.put(2L, new Double[] {1.2579416330459492, 1.0406035275510874, 0.7808214813428701, 0.4118128641127321});
		for (Row row : recommendItems) {
			List<Row> rows = ((MTable) row.getField(2)).getRows();

			Double[] actual = new Double[rows.size()];
			for (int i = 0; i < rows.size(); ++i) {
				actual[i] = (double) rows.get(i).getField(1);
			}

			Double[] expect = score.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}

		Map <Object, Double[]> similarity = new HashMap <>();
		similarity.put("a", new Double[] {0.8846153846153848, 0.08770580193070288});
		similarity.put("b", new Double[] {0.8846153846153848, 0.4385290096535146, 0.19611613513818404});
		similarity.put("c", new Double[] {0.8944271909999159, 0.4385290096535146, 0.08770580193070288});
		similarity.put("d", new Double[] {0.8944271909999159, 0.19611613513818404});

		List <Row> similarItems = new ItemCfSimilarItemsRecommBatchOp()
			.setItemCol("item")
			.setRecommCol("recomm")
			.linkFrom(trainBatchOp, zipItem)
			.collect();

		for (Row row : similarItems) {
			List<Row> rows = ((MTable) row.getField(2)).getRows();

			Double[] actual = new Double[rows.size()];
			for (int i = 0; i < rows.size(); ++i) {
				actual[i] = (double) rows.get(i).getField(1);
			}
			Double[] expect = similarity.get(row.getField(0));
			for (int i = 0; i < actual.length; i++) {
				Assert.assertEquals(actual[i], expect[i], 0.01);
			}
		}
	}

	@Test
	public void testPearson() {
		BatchOperator<?> emptyRate = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"user", "item", "rate"}));

		BatchOperator<?> spliter = new LeaveTopKObjectOutBatchOp()
			.setK(2)
			.setObjectCol("item")
			.setRateCol("rate")
			.setOutputCol("label")
			.setGroupCol("user");

		BatchOperator<?> test = spliter.linkFrom(emptyRate);
		BatchOperator<?> train = spliter.getSideOutput(0);

		ItemCfTrainBatchOp trainBatchOp = new ItemCfTrainBatchOp()
			.setSimilarityType("PEARSON")
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rate")
			.linkFrom(train);

		ItemCfItemsPerUserRecommender recommender = new ItemCfItemsPerUserRecommender()
			.setUserCol("user")
			.setRecommCol("recomm")
			.setModelData(trainBatchOp);

		PipelineModel model = new Pipeline()
			.add(recommender)
			.fit(trainBatchOp);

		model.transform(test).collect();
	}

	@Test
	public void testLazyPrint() throws Exception {
		BatchOperator<?> emptyRate = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows, new String[] {"user", "item", "rate"}));

		ItemCfTrainBatchOp trainBatchOp = new ItemCfTrainBatchOp()
			.setSimilarityType("PEARSON")
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rate")
			.linkFrom(emptyRate);

		trainBatchOp.lazyPrintModelInfo();
		BatchOperator.execute();
	}

	@Test
	public void testLocalPredictor() throws Exception {
		BatchOperator<?> data = BatchOperator.fromTable(
			MLEnvironmentFactory.getDefault().createBatchTable(rows1, new String[] {"user", "item", "rate"}));

		ItemCfTrainBatchOp trainBatchOp = new ItemCfTrainBatchOp()
			.setSimilarityType("COSINE")
			.setUserCol("user")
			.setItemCol("item")
			.setRateCol("rate")
			.linkFrom(data);

		ItemCfItemsPerUserRecommender recommender = new ItemCfItemsPerUserRecommender()
			.setUserCol("user")
			.setRecommCol("recomm")
			.setModelData(trainBatchOp);

		Row res = recommender.collectLocalPredictor("user long, item long, rate double").map(rows1[0]);

		List<Row> rows = ((MTable) res.getField(3)).getRows();

		List <Long> recomm = new ArrayList <>(rows.size());
		for (Row row : rows) {
			recomm.add((long)row.getField(0));
		}
		Assert.assertEquals(JsonConverter.toJson(recomm), "[2,1,3]");
	}
}