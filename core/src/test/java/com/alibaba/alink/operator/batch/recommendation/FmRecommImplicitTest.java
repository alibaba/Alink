package com.alibaba.alink.operator.batch.recommendation;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

public class FmRecommImplicitTest extends AlinkTestBase {
	Row[] rows = new Row[] {
		Row.of("1L", "1L", 1.0),
		Row.of("2L", "2L", 1.0),
		Row.of("2L", "3L", 0.0),
		Row.of("3L", "1L", 0.0),
		Row.of("3L", "2L", 1.0),
		Row.of("3L", "3L", 1.0),
	};

	Row[] itemfeat = new Row[] {
		Row.of("1L", "1"),
		Row.of("2L", "3"),
		Row.of("3L", "1")
	};

	private static void eval(BatchOperator pred) {
		pred = pred.select(
			"label, concat('{\"0\":', cast((1-p) as varchar), ',\"1\":', cast(p as varchar), '}') as p_detail");

		EvalBinaryClassBatchOp eval = new EvalBinaryClassBatchOp()
			.setLabelCol("label").setPredictionDetailCol("p_detail")
			.linkFrom(pred);

		BinaryClassMetrics metrics = eval.collectMetrics();
		System.out.println(
			String.format("auc=%f,acc=%f,f1=%f", metrics.getAuc(), metrics.getAccuracy(), metrics.getF1()));
	}

	@Test
	public void test() throws Exception {
		BatchOperator trainData = new MemSourceBatchOp(rows, new String[] {"user", "item", "label"});
		BatchOperator itemFeatures = new MemSourceBatchOp(itemfeat, new String[] {"item", "features"});

		FmRecommBinaryImplicitTrainBatchOp fmTrain = new FmRecommBinaryImplicitTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setItemFeatureCols(new String[] {"features"})
			.setLearnRate(0.1)
			.setNumEpochs(10)
			.setLambda0(0.01)
			.setLambda1(0.01)
			.setLambda2(0.01)
			.setWithIntercept(true)
			.setWithLinearItem(true)
			.setNumFactor(5);

		fmTrain.linkFrom(trainData, null, itemFeatures);

		FmRateRecommBatchOp predictor = new FmRateRecommBatchOp()
			.setUserCol("user").setItemCol("item")
			.setRecommCol("p");

		BatchOperator pred = predictor.linkFrom(fmTrain, trainData);
		eval(pred);
	}
}
