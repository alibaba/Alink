package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class OnlineLearningTest extends AlinkTestBase {

	AlgoOperator getData() {
		Row[] array = new Row[] {
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", 1.0, 1.0, 1.0, 1.0, 1),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", 1.0, 1.0, 0.0, 1.0, 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", 1.0, 0.0, 1.0, 1.0, 1),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", 0.0, 1.0, 1.0, 0.0, 0)
		};

		return new MemSourceBatchOp(
			Arrays.asList(array), new String[] {"svec", "vec", "f0", "f1", "f2", "f3", "labels"});
	}

	@Test
	public void Test() throws Exception {
		String[] xVars = new String[] {"f0", "f1", "f2", "f3"};
		String yVar = "labels";

		BatchOperator trainData = (BatchOperator) getData();

		LogisticRegressionTrainBatchOp lr = new LogisticRegressionTrainBatchOp()
			.setLabelCol(yVar)
			.setFeatureCols(xVars)
			.setOptimMethod("lbfgs").linkFrom(trainData);

		FtrlTrainStreamOp ftrl = new FtrlTrainStreamOp(lr).setAlpha(0.1).setBeta(0.1).setL1(0.1).setL2(0.1)
			.setFeatureCols(xVars).setLabelCol(yVar).setTimeInterval(1).setWithIntercept(false);

		Assert.assertEquals(0.1, ftrl.getL1(), 1.0e-5);
		Assert.assertEquals(0.1, ftrl.getL2(), 1.0e-5);
		Assert.assertEquals(0.1, ftrl.getAlpha(), 1.0e-5);
		Assert.assertEquals(0.1, ftrl.getBeta(), 1.0e-5);
		Assert.assertEquals(1, ftrl.getTimeInterval(), 1.0e-5);
		Assert.assertEquals(false, ftrl.getWithIntercept());
	}
}
