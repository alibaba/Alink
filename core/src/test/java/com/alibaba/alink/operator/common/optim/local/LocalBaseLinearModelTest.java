package com.alibaba.alink.operator.common.optim.local;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.operator.common.linear.LinearModelData;
import com.alibaba.alink.operator.common.linear.LocalLinearRegression;
import com.alibaba.alink.operator.common.linear.LocalLogistRegression;
import com.alibaba.alink.operator.common.optim.ConstraintBetweenFeatures;
import com.alibaba.alink.operator.common.optim.FeatureConstraint;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.LinearTrainParams;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class LocalBaseLinearModelTest extends AlinkTestBase {
	Params optParams = new Params()
		.set(LinearTrainParams.WITH_INTERCEPT, true)
		.set(LinearTrainParams.STANDARDIZATION, false)
		.set(HasL1.L_1, 0.)
		.set(HasL2.L_2, 1.);

	@Test
	public void testLinearReg() {

		List <Tuple3 <Double, Object, Vector>> trainData = new ArrayList <>();
		trainData.add(Tuple3.of(1., 16.8, new DenseVector(new double[] {1.0, 7.0, 9.0})));
		trainData.add(Tuple3.of(1., 6.7, new DenseVector(new double[] {1.0, 3.0, 3.0})));
		trainData.add(Tuple3.of(1., 6.9, new DenseVector(new double[] {1.0, 2.0, 4.0})));
		trainData.add(Tuple3.of(1., 8.0, new DenseVector(new double[] {1.0, 3.0, 4.0})));

		List <Vector> predictData = new ArrayList <>();
		predictData.add(new DenseVector(new double[] {1.0, 7.0, 9.0}));
		predictData.add(new DenseVector(new double[] {1.0, 3.0, 3.0}));
		predictData.add(new DenseVector(new double[] {1.0, 2.0, 4.0}));
		predictData.add(new DenseVector(new double[] {1.0, 3.0, 4.0}));

		boolean hasIntercept = true;
		ConstraintBetweenFeatures constraint = new ConstraintBetweenFeatures();
		constraint.addLessThanFeature(0, 1);
		constraint.addLargerThan(1, 0.5);
		FeatureConstraint f = new FeatureConstraint();
		f.addConstraintBetweenFeature(constraint);

		LinearModelData model = LocalLinearRegression.train(trainData,
			null,
			hasIntercept,
			0,
			0,
			"lbfgs",
			f.toString());

		List <Tuple2 <Object, Vector>> predicted = LocalLinearRegression.predict(model, predictData);

		for (Tuple2 <Object, Vector> value : predicted) {
			System.out.println(value);
		}
	}

	@Test
	public void testLogisticRegression() {
		List <Tuple3 <Double, Object, Vector>> trainData = new ArrayList <>();
		trainData.add(Tuple3.of(1.0, 1., new DenseVector(new double[] {1.0, 1.0, 1.0, 1.0})));
		trainData.add(Tuple3.of(1.0, 1., new DenseVector(new double[] {1.0, 1.0, 0.0, 1.0})));
		trainData.add(Tuple3.of(1.0, 1., new DenseVector(new double[] {1.0, 0.0, 1.0, 1.0})));
		trainData.add(Tuple3.of(1.0, 1., new DenseVector(new double[] {1.0, 0.0, 1.0, 1.0})));
		trainData.add(Tuple3.of(1.0, 0., new DenseVector(new double[] {0.0, 1.0, 1.0, 0.0})));
		trainData.add(Tuple3.of(1.0, 0., new DenseVector(new double[] {0.0, 1.0, 1.0, 0.0})));
		trainData.add(Tuple3.of(1.0, 0., new DenseVector(new double[] {0.0, 1.0, 1.0, 0.0})));
		trainData.add(Tuple3.of(1.0, 0., new DenseVector(new double[] {0.0, 1.0, 1.0, 0.0})));

		List <Vector> predictData = new ArrayList <>();
		predictData.add(new DenseVector(new double[] {1.0, 1.0, 1.0, 1.0}));
		predictData.add(new DenseVector(new double[] {1.0, 1.0, 0.0, 1.0}));
		predictData.add(new DenseVector(new double[] {1.0, 0.0, 1.0, 1.0}));
		predictData.add(new DenseVector(new double[] {1.0, 0.0, 1.0, 1.0}));

		boolean hasIntercept = true;
		LinearModelData model = LocalLogistRegression.train(trainData,
			null,
			hasIntercept,
			0,
			1,
			null,
			null);

		System.out.println(model.coefVector);

		List <Tuple2 <Object, Vector>> predicted = LocalLinearRegression.predict(model, predictData);

		for (Tuple2 <Object, Vector> value : predicted) {
			System.out.println(value);
		}
	}

}
