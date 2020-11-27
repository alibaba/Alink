package com.alibaba.alink.operator.batch.recommendation;

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
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.UnionAllStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AlsTrainBatchOpTest extends AlinkTestBase {
	Row[] rows1 = new Row[] {
		Row.of("1L", "1L", 5.0),
		Row.of("2L", "2L", 1.0),
		Row.of("2L", "3L", 2.0),
		Row.of("3L", "1L", 1.0),
		Row.of("3L", "2L", 3.0),
		Row.of("3L", "3L", 0.0),
	};
	Row[] rows2 = new Row[] {
		Row.of(1L, 1L, 5.0),
		Row.of(2L, 2L, 1.0),
		Row.of(2L, 3L, 2.0),
		Row.of(3L, 1L, 1.0),
		Row.of(3L, 2L, 3.0),
		Row.of(3L, 3L, 0.0),
	};

	private BatchOperator train() throws Exception {
		Long envId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;
		BatchOperator samples = new MemSourceBatchOp(rows1, new String[] {"uid", "iid", "label"}).setMLEnvironmentId(
			envId);

		AlsTrainBatchOp alsOp = new AlsTrainBatchOp()
			.setMLEnvironmentId(envId)
			.setUserCol("uid")
			.setItemCol("iid")
			.setRateCol("label");

		BatchOperator model = alsOp.linkFrom(samples);
		return model;
	}

	@Test
	public void testLazyPrint() throws Exception {
		AlsTrainBatchOp als = (AlsTrainBatchOp) train();
		als.print();
		als.lazyPrintModelInfo("ALS");
		BatchOperator.execute();
	}

	@Test
	public void testPredict() throws Exception {
		Long envId = MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID;
		BatchOperator samples = new MemSourceBatchOp(rows1, new String[] {"uid", "iid", "label"}).setMLEnvironmentId(
			envId);

		BatchOperator model = train();
		AlsRateRecommBatchOp predictor1 = new AlsRateRecommBatchOp()
			.setMLEnvironmentId(envId)
			.setUserCol("uid").setItemCol("iid").setRecommCol("p");
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
		BatchOperator result1 = predictor1.linkFrom(model, samples);
		BatchOperator result2 = predictor2.linkFrom(model, samples);
		BatchOperator result3 = predictor3.linkFrom(model, samples);
		BatchOperator result4 = predictor4.linkFrom(model, samples);
		BatchOperator result5 = predictor5.linkFrom(model, samples);

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
		StreamOperator streamsamples = new MemSourceStreamOp(rows1, new String[] {"uid", "iid", "label"})
			.setMLEnvironmentId(envId);

		BatchOperator model = train();
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
		StreamOperator result1 = predictor1.linkFrom(streamsamples);
		StreamOperator result2 = predictor2.linkFrom(streamsamples);
		StreamOperator result3 = predictor3.linkFrom(streamsamples);
		StreamOperator result4 = predictor4.linkFrom(streamsamples);
		StreamOperator result5 = predictor5.linkFrom(streamsamples);

		result2 = result2.select("*, 'AlsItemsPerUserRecommStreamOp' as rec_type");
		result3 = result3.select("*, 'AlsUsersPerItemRecommStreamOp' as rec_type");
		result4 = result4.select("*, 'AlsSimilarUsersRecommStreamOp' as rec_type");
		result5 = result5.select("*, 'AlsSimilarItemsRecommStreamOp' as rec_type");

		result1.print();
		new UnionAllStreamOp().linkFrom(result2, result3, result4, result5).print();
		StreamOperator.execute();
	}
}