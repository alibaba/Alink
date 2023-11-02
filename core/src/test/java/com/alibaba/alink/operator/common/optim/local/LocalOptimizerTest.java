package com.alibaba.alink.operator.common.optim.local;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.linear.UnaryLossObjFunc;
import com.alibaba.alink.operator.common.linear.unarylossfunc.LogLossFunc;
import com.alibaba.alink.operator.common.optim.LocalOptimizer;
import com.alibaba.alink.operator.common.optim.objfunc.OptimObjFunc;
import com.alibaba.alink.params.shared.clustering.HasEpsilon;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import com.alibaba.alink.params.shared.linear.LinearTrainParams.OptimMethod;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LocalOptimizerTest extends AlinkTestBase {
	@Test
	public void testLocalSqp() throws Exception {
		OptimObjFunc objFunc = new UnaryLossObjFunc(new LogLossFunc(), new Params());

		DataSet tmp = new MemSourceBatchOp(
			new Object[][] {
				{"1.1 2.0", 1.0},
				{"2.1 3.1", 1.0},
				{"3.1 2.2", 1.0},
				{"1.2 3.2", 0.0},
				{"1.2 4.2", 0.0}
			},
			new String[] {"vec", "label"}).getDataSet();

		List <Tuple3 <Double, Double, Vector>> trainData = new ArrayList <>();
		trainData.add(Tuple3.of(1., 1.0, new DenseVector(new double[] {1.0, 7.0, 9.0})));
		trainData.add(Tuple3.of(1., 1.0, new DenseVector(new double[] {1.0, 3.0, 3.0})));
		trainData.add(Tuple3.of(1., 0.0, new DenseVector(new double[] {1.0, 2.0, 4.0})));
		trainData.add(Tuple3.of(1., 0.0, new DenseVector(new double[] {1.0, 3.0, 4.0})));

		DenseVector weight = LocalOptimizer.optimize(objFunc, trainData, new DenseVector(3),
			new Params().set(HasEpsilon.EPSILON, 0.001).set(LinearTrainParams.OPTIM_METHOD, OptimMethod.GD)).f0;
		System.out.println(weight);
		DenseVector weight1 = LocalOptimizer.optimize(objFunc, trainData, new DenseVector(3),
			new Params().set(HasEpsilon.EPSILON, 0.001).set(LinearTrainParams.OPTIM_METHOD, OptimMethod.SGD)).f0;
		System.out.println(weight1);
		DenseVector weight2 = LocalOptimizer.optimize(objFunc, trainData, new DenseVector(3),
			new Params().set(HasEpsilon.EPSILON, 0.001).set(LinearTrainParams.OPTIM_METHOD, OptimMethod.LBFGS)).f0;
		System.out.println(weight2);
		DenseVector weight3 = LocalOptimizer.optimize(objFunc, trainData, new DenseVector(3),
			new Params().set(HasL1.L_1, 0.00001).set(HasEpsilon.EPSILON, 0.001)
				.set(LinearTrainParams.OPTIM_METHOD, OptimMethod.OWLQN)).f0;
		System.out.println(weight3);
		DenseVector weight4 = LocalOptimizer.optimize(objFunc, trainData, new DenseVector(3),
			new Params().set(HasEpsilon.EPSILON, 0.001).set(LinearTrainParams.OPTIM_METHOD, OptimMethod.Newton)).f0;
		System.out.println(weight4);
	}
}