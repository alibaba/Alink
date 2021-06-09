package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RegressionMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class FmRecommTest extends AlinkTestBase {
	Row[] rows = new Row[] {
		Row.of("1L", "1L", 5.0),
		Row.of("2L", "2L", 1.0),
		Row.of("2L", "3L", 2.0),
		Row.of("3L", "1L", 1.0),
		Row.of("3L", "2L", 3.0),
		Row.of("3L", "3L", 0.0),
	};

	@Test
	public void test() throws Exception {

		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"user_id", "item_id", "rating"});

		FmRecommTrainBatchOp fmTrain = new FmRecommTrainBatchOp()
			.setUserCol("user_id").setItemCol("item_id")
			.setLambda2(0.2).setWithIntercept(true).setWithLinearItem(true).setNumFactor(20)
			.setRateCol("rating");
		fmTrain.linkFrom(data);

		FmRateRecommBatchOp predictor = new FmRateRecommBatchOp()
			.setUserCol("user_id").setItemCol("item_id")
			.setRecommCol("prediction_result");

		predictor.linkFrom(fmTrain, data);

		EvalRegressionBatchOp eval = new EvalRegressionBatchOp()
			.setLabelCol("rating").setPredictionCol("prediction_result")
			.linkFrom(predictor);

		RegressionMetrics metrics = eval.collectMetrics();
		System.out.println(String.format("mse=%f, mae=%f", metrics.getMse(), metrics.getMae()));
	}

	@Test
	public void testFMPred() throws Exception {
		BatchOperator data = new MemSourceBatchOp(rows, new String[] {"user_id", "item_id", "rating"});

		FmRecommTrainBatchOp fmTrain = new FmRecommTrainBatchOp()
			.setUserCol("user_id").setItemCol("item_id")
			.setRateCol("rating")
			.setNumEpochs(10);
		fmTrain.linkFrom(data);

		FmItemsPerUserRecommBatchOp recUser = new FmItemsPerUserRecommBatchOp()
			.setUserCol("user_id").setRecommCol("rec").setNumThreads(4);
		recUser.linkFrom(fmTrain, data).lazyPrint(10);

		FmUsersPerItemRecommBatchOp recItem = new FmUsersPerItemRecommBatchOp()
			.setItemCol("item_id").setRecommCol("rec").setNumThreads(4);
		recItem.linkFrom(fmTrain, data).lazyPrint(10);
		BatchOperator.execute();
	}
}
