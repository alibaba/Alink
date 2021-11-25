package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.IsotonicRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

/**
 * Unit test of IsotonicRegression algorithm.
 */

public class IsotonicRegTrainBatchOpTest extends AlinkTestBase {
	private Tuple4[] tuples = new Tuple4[] {
		Tuple4.of(0.0, 0.3, 0.3, 1.0),
		Tuple4.of(1.0, 0.27, 0.27, 1.0),
		Tuple4.of(1.0, 0.55, 0.55, 1.0),
		Tuple4.of(1.0, 0.5, 0.5, 1.0),
		Tuple4.of(0.0, 0.2, 0.2, 1.0),
		Tuple4.of(0.0, 0.18, 0.18, 1.0),
		Tuple4.of(1.0, 0.1, 0.1, 1.0),
		Tuple4.of(0.0, 0.45, 0.45, 1.0),
		Tuple4.of(1.0, 0.9, 0.9, 1.0),
		Tuple4.of(1.0, 0.8, 0.8, 1.0),
		Tuple4.of(0.0, 0.7, 0.7, 1.0),
		Tuple4.of(1.0, 0.6, 0.6, 1.0),
		Tuple4.of(1.0, 0.35, 0.35, 1.0),
		Tuple4.of(1.0, 0.4, 0.4, 1.0),
		Tuple4.of(1.0, 0.02, 0.02, 1.0)
	};
	private Tuple4[] tupleTest = new Tuple4[] {
		Tuple4.of(1.0, 0.02, 0.02, 1.0),
		Tuple4.of(1.0, 0.40, 0.40, 1.0),
		Tuple4.of(1.0, 0.48, 0.48, 1.0),
		Tuple4.of(1.0, 0.49, 0.49, 1.0),
		Tuple4.of(1.0, 0.33, 0.33, 1.0),
		Tuple4.of(1.0, 0.75, 0.75, 1.0),
		Tuple4.of(1.0, 1.00, 1.00, 1.0),
		Tuple4.of(1.0, 1.02, 1.02, 1.0),
		Tuple4.of(1.0, 0.01, 0.01, 1.0),
		Tuple4.of(1.0, 0.00, 0.00, 1.0)
	};

	@Test
	public void isotonicRegTest() throws Exception {
		int length = 15;
		Object[][] inTrain = new Object[length][4];
		for (int i = 0; i < length; ++i) {
			inTrain[i][0] = tuples[i].f0;
			inTrain[i][1] = tuples[i].f1;
			inTrain[i][2] = tuples[i].f2;
			inTrain[i][3] = tuples[i].f3;
		}
		Object[][] inTest = new Object[10][4];
		for (int i = 0; i < 10; ++i) {
			inTest[i][0] = tupleTest[i].f0;
			inTest[i][1] = tupleTest[i].f1;
			inTest[i][2] = tupleTest[i].f2;
			inTest[i][3] = tupleTest[i].f3;
		}
		String[] colNames = new String[] {"col1", "col2", "col3", "weight"};
		MemSourceBatchOp trainData = new MemSourceBatchOp(inTrain, colNames);
		MemSourceBatchOp predictData = new MemSourceBatchOp(inTest, new String[] {"col1", "col2", "weight", "col3"});
		StreamOperator streamData = new MemSourceStreamOp(inTest, new String[] {"col1", "col2", "weight", "col3"});
		IsotonicRegTrainBatchOp model = new IsotonicRegTrainBatchOp()
			.setLabelCol("col1").setFeatureCol("col2").setWeightCol("col3")
			.linkFrom(trainData);
		IsotonicRegPredictBatchOp predictBatchOp = new IsotonicRegPredictBatchOp().setPredictionCol("predictCol");
		new IsotonicRegTrainBatchOp()
			.setLabelCol("col1").setVectorCol("col2").setFeatureIndex(0).setWeightCol("weight")
			.linkFrom(trainData).lazyCollect();

		IsotonicRegTrainBatchOp model2 = new IsotonicRegTrainBatchOp()
			.setLabelCol("col1").setVectorCol("col2").setFeatureIndex(0).linkFrom(trainData);
		new IsotonicRegPredictStreamOp(model).setPredictionCol("predictCol").linkFrom(streamData).print();
		BatchOperator res = predictBatchOp.linkFrom(model2, predictData);
		res.lazyPrint(-1);
		new IsotonicRegPredictBatchOp().setPredictionCol("predictCol")
			.linkFrom(model2, predictData).print();
		StreamOperator.execute();

	}

}