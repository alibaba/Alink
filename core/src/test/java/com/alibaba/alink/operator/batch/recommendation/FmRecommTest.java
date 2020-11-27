package com.alibaba.alink.operator.batch.recommendation;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class FmRecommTest extends AlinkTestBase {
	@Test
	public void test() throws Exception {

		FmRecommTrainBatchOp fmTrain = new FmRecommTrainBatchOp()
			.setUserCol("user_id").setItemCol("item_id")
			.setLambda2(0.2).setWithIntercept(true).setWithLinearItem(true).setNumFactor(20)
			.setRateCol("rating");
		fmTrain.linkFrom(MovieLens100k.getRatingsTrainData());

		FmRateRecommBatchOp predictor = new FmRateRecommBatchOp()
			.setUserCol("user_id").setItemCol("item_id")
			.setRecommCol("prediction_result");

		predictor.linkFrom(fmTrain, MovieLens100k.getRatingsTestData());

		EvalRegressionBatchOp eval = new EvalRegressionBatchOp()
			.setLabelCol("rating").setPredictionCol("prediction_result")
			.linkFrom(predictor);

		RegressionMetrics metrics = eval.collectMetrics();
		System.out.println(String.format("mse=%f, mae=%f", metrics.getMse(), metrics.getMae()));
	}

	@Test
	public void testFMPred() throws Exception {
		FmRecommTrainBatchOp fmTrain = new FmRecommTrainBatchOp()
				.setUserCol("user_id").setItemCol("item_id")
				.setRateCol("rating")
				.setNumEpochs(10);
		fmTrain.linkFrom(MovieLens100k.getRatingsTrainData());

		FmItemsPerUserRecommBatchOp recUser = new FmItemsPerUserRecommBatchOp()
				.setUserCol("user_id").setRecommCol("rec").setNumThreads(4);
		recUser.linkFrom(fmTrain, MovieLens100k.getRatingsTestData()).lazyPrint(10);

		FmUsersPerItemRecommBatchOp recItem = new FmUsersPerItemRecommBatchOp()
				.setItemCol("item_id").setRecommCol("rec").setNumThreads(4);
		recItem.linkFrom(fmTrain, MovieLens100k.getRatingsTestData()).lazyPrint(10);
		BatchOperator.execute();
	}
}
