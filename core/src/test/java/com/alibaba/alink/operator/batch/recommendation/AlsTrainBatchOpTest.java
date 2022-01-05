package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionAllBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.recommendation.AlsItemsPerUserRecommStreamOp;
import com.alibaba.alink.operator.stream.recommendation.AlsRateRecommStreamOp;
import com.alibaba.alink.operator.stream.recommendation.AlsSimilarItemsRecommStreamOp;
import com.alibaba.alink.operator.stream.recommendation.AlsSimilarUsersRecommStreamOp;
import com.alibaba.alink.operator.stream.recommendation.AlsUsersPerItemRecommStreamOp;
import com.alibaba.alink.operator.stream.sink.CollectSinkStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.UnionAllStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class AlsTrainBatchOpTest extends AlinkTestBase {
	Row[] rows1 = new Row[] {
		Row.of("1L", "5L", 5.0),
		Row.of("2L", "6L", 1.0),
		Row.of("2L", "7L", 2.0),
		Row.of("3L", "8L", 1.0),
		Row.of("3L", "9L", 3.0),
		Row.of("3L", "6L", 0.0),
	};

	private AlsTrainBatchOp train() {
		Long envId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;
		BatchOperator<?> samples = new MemSourceBatchOp(rows1, new String[] {"uid", "iid", "label"}).setMLEnvironmentId(
			envId);

		AlsTrainBatchOp alsOp = new AlsTrainBatchOp()
			.setMLEnvironmentId(envId)
			.setUserCol("uid")
			.setItemCol("iid")
			.setRateCol("label");

		return alsOp.linkFrom(samples);
	}

	@Test
	public void testUserItemExtraction() throws Exception {
		AlsTrainBatchOp model = train();
		Params params = new Params();
		AlsModelInfoBatchOp transformModel = new AlsModelInfoBatchOp(params).linkFrom(model);
		List<Row> userEmbeddomg = transformModel.getUserEmbedding().collect();
		for (Row row : userEmbeddomg) {
			if (row.getField(0).equals(1L)) {
				Assert.assertEquals(row.getField(1),"0.36496326327323914 0.2768574655056 0.4218851923942566 0.6714380383491516 1.0786648988723755 1.0764501094818115 0.9629398584365845 0.025280786678195 1.0309195518493652 0.19274161756038666");
			}
		}
		List<Row> itemEmbeddomg = transformModel.getItemEmbedding().collect();
		for (Row row : itemEmbeddomg) {
			if (row.getField(0).equals(5L)) {
				Assert.assertEquals(row.getField(1),"0.34503644704818726 0.26174119114875793 0.3988504707813263 0.634777843952179 1.0197702646255493 1.0176764726638794 0.9103637933731079 0.02390046790242195 0.9746318459510803 0.1822180151939392");
			}
		}
	}

	@Test
	public void testLazyPrint() throws Exception {
		AlsTrainBatchOp als = train();
		als.print();
		als.lazyPrintModelInfo("ALS");
		BatchOperator.execute();
	}

	@Test
	public void testPredict() {
		Long envId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;
		BatchOperator<?> samples = new MemSourceBatchOp(rows1, new String[] {"uid", "iid", "label"}).setMLEnvironmentId(
			envId);

		BatchOperator<?> model = train();
		AlsItemsPerUserRecommBatchOp predictor2 = new AlsItemsPerUserRecommBatchOp()
			.setMLEnvironmentId(envId)
			.setExcludeKnown(true)
			.setUserCol("uid").setRecommCol("p");
		AlsUsersPerItemRecommBatchOp predictor3 = new AlsUsersPerItemRecommBatchOp()
			.setMLEnvironmentId(envId)
			.setItemCol("iid").setRecommCol("p");
		AlsSimilarUsersRecommBatchOp predictor4 = new AlsSimilarUsersRecommBatchOp()
			.setMLEnvironmentId(envId)
			.setUserCol("uid").setRecommCol("p");
		AlsSimilarItemsRecommBatchOp predictor5 = new AlsSimilarItemsRecommBatchOp()
			.setMLEnvironmentId(envId)
			.setItemCol("iid").setRecommCol("p");
		BatchOperator<?> result2 = predictor2.linkFrom(model, samples);
		BatchOperator<?> result3 = predictor3.linkFrom(model, samples);
		BatchOperator<?> result4 = predictor4.linkFrom(model, samples);
		BatchOperator<?> result5 = predictor5.linkFrom(model, samples);

		result2 = result2.select("*, 'AlsItemsPerUserRecommBatchOp' as rec_type");
		result3 = result3.select("*, 'AlsUsersPerItemRecommBatchOp' as rec_type");
		result4 = result4.select("*, 'AlsSimilarUsersRecommBatchOp' as rec_type");
		result5 = result5.select("*, 'AlsSimilarItemsRecommBatchOp' as rec_type");

		int s = new UnionAllBatchOp().setMLEnvironmentId(envId).linkFrom(result2, result3, result4, result5).collect()
			.size();
		Assert.assertEquals(s, 24);
	}

	@Test
	public void testStreamPredict() throws Exception {
		Long envId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;
		StreamOperator<?> streamsamples = new MemSourceStreamOp(rows1, new String[] {"uid", "iid", "label"})
			.setMLEnvironmentId(envId);

		BatchOperator<?> model = train();
		AlsRateRecommStreamOp predictor1 = new AlsRateRecommStreamOp(model)
			.setUserCol("uid").setItemCol("iid").setRecommCol("p");
		AlsItemsPerUserRecommStreamOp predictor2 = new AlsItemsPerUserRecommStreamOp(model)
			.setUserCol("uid").setRecommCol("p");
		AlsUsersPerItemRecommStreamOp predictor3 = new AlsUsersPerItemRecommStreamOp(model)
			.setItemCol("iid").setRecommCol("p");
		AlsSimilarUsersRecommStreamOp predictor4 = new AlsSimilarUsersRecommStreamOp(model)
			.setUserCol("uid").setRecommCol("p");
		AlsSimilarItemsRecommStreamOp predictor5 = new AlsSimilarItemsRecommStreamOp(model)
			.setItemCol("iid").setRecommCol("p");
		StreamOperator<?> result1 = predictor1.linkFrom(streamsamples);
		StreamOperator<?> result2 = predictor2.linkFrom(streamsamples);
		StreamOperator<?> result3 = predictor3.linkFrom(streamsamples);
		StreamOperator<?> result4 = predictor4.linkFrom(streamsamples);
		StreamOperator<?> result5 = predictor5.linkFrom(streamsamples);

		result2 = result2.select("*, 'AlsItemsPerUserRecommStreamOp' as rec_type");
		result3 = result3.select("*, 'AlsUsersPerItemRecommStreamOp' as rec_type");
		result4 = result4.select("*, 'AlsSimilarUsersRecommStreamOp' as rec_type");
		result5 = result5.select("*, 'AlsSimilarItemsRecommStreamOp' as rec_type");

		result1.print();

		CollectSinkStreamOp sop = new UnionAllStreamOp()
			.linkFrom(result2, result3, result4, result5).link(new CollectSinkStreamOp());
		StreamOperator.execute();
		List<Row> res = sop.getAndRemoveValues();
		Assert.assertEquals(res.size(), 24);
	}
}