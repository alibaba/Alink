package com.alibaba.alink.operator.common.optim.local;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.unarylossfunc.SquareLossFunc;
import com.alibaba.alink.operator.common.optim.activeSet.ConstraintObjFunc;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LocalSqpTest extends AlinkTestBase {
	@Test
	public void testLocalSqp() {
		List <Tuple3 <Double, Double, Vector>> trainData = new ArrayList <>();
		trainData.add(Tuple3.of(1., 16.8, new DenseVector(new double[] {1.0, 7.0, 9.0})));
		trainData.add(Tuple3.of(1., 6.7, new DenseVector(new double[] {1.0, 3.0, 3.0})));
		trainData.add(Tuple3.of(1., 6.9, new DenseVector(new double[] {1.0, 2.0, 4.0})));
		trainData.add(Tuple3.of(1., 8.0, new DenseVector(new double[] {1.0, 3.0, 4.0})));
		DenseVector weight = ConstrainedLocalOptimizer
			.optimizeWithHessian(trainData, LinearModelType.LinearReg, new Params()).f0;
		System.out.println(weight.toString());
		List <Double> res = new ArrayList <>();
		for (Tuple3 <Double, Double, Vector> trainDatum : trainData) {
			res.add(trainDatum.f2.dot(weight));
		}
		System.out.println(res);
	}

	@Test
	public void testSqp() {
		List <Tuple3 <Double, Double, Vector>> trainData = new ArrayList <>();
		trainData.add(Tuple3.of(1., 16.8, new DenseVector(new double[] {1.0, 7.0, 9.0})));
		trainData.add(Tuple3.of(1., 6.7, new DenseVector(new double[] {1.0, 3.0, 3.0})));
		trainData.add(Tuple3.of(1., 6.9, new DenseVector(new double[] {1.0, 2.0, 4.0})));
		trainData.add(Tuple3.of(1., 8.0, new DenseVector(new double[] {1.0, 3.0, 4.0})));
		DenseVector iniVec = new DenseVector(new double[] {0.1, 0.1, 0.1});
		Params params = new Params().set(HasWithIntercept.WITH_INTERCEPT, true);
		ConstraintObjFunc objFunc = new ConstraintObjFunc(new SquareLossFunc(), params);
		System.out.println(LocalSqp.sqp(trainData, iniVec, params, objFunc));
	}

}