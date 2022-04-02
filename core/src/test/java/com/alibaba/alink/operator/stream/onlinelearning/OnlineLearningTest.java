package com.alibaba.alink.operator.stream.onlinelearning;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp.FtrlLearningKernel;
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

		FtrlLearningKernel kernel = new FtrlLearningKernel();
		kernel.setModelParams(new Params(), 2, new Object[] {1, 0});

		kernel.calcLocalWx(new double[] {1, 2}, new DenseVector(2), 0);
		kernel.getFeedbackVar(new double[] {1, 2});
		double[] coef = new double[] {2.0, 3.0};

		kernel.updateModel(coef, new DenseVector(2), new double[] {1, 1}, 1L, 0, 0);
		SparseVector svec = new SparseVector(2);
		svec.add(0, 1);
		svec.add(1, 1);
		kernel.updateModel(coef, svec, new double[] {1, 1}, 1L, 0, 0);
		ftrl.setLearningKernel(kernel);
		Assert.assertEquals(coef[0], -0.08761006569007045, 0.0001);
		Assert.assertEquals(coef[1], -0.08761006569007045, 0.0001);
	}
}
