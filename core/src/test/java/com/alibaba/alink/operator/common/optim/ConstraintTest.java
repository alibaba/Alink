package com.alibaba.alink.operator.common.optim;

import org.apache.flink.api.java.tuple.Tuple4;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ConstraintTest extends AlinkTestBase {
	@Test
	public void testConstraint() {
		ConstraintBetweenBins constraints = new ConstraintBetweenBins();
		constraints.setDim(20);
		constraints.addEqual(new Number[] {0, 10.});
		constraints.addEqual(new Number[] {1, 11.});
		constraints.addLargerThan(new Number[] {2, 2.});
		constraints.addLessThan(new Number[] {3, 5.});
		constraints.addScale(new Number[] {3, 4, 6.});
		constraints.addLargerThanBin(new Number[] {1, 2, 3});
		constraints.addLargerThanBin(new Number[] {2, 3});
		constraints.addLessThanBin(new Number[] {0, 4});
		constraints.addLessThanBin(new Number[] {0, 1});
		String consJson = JsonConverter.toJson(constraints);
		System.out.println(consJson);
		ConstraintBetweenBins constraintFromJson = ConstraintBetweenBins.fromJson(consJson);
		constraintFromJson.setName("name");
		System.out.println(constraintFromJson.toString());
		Tuple4 <double[][], double[], double[][], double[]> cons = constraintFromJson.getConstraints(5);
		System.out.println("inequalMatrix");
		System.out.println(new DenseMatrix(cons.f0).toString());
		System.out.println("inequalVector");
		System.out.println(new DenseVector(cons.f1).toString());
		System.out.println("equalMatrix");
		System.out.println(new DenseMatrix(cons.f2).toString());
		System.out.println("equalVector");
		System.out.println(new DenseVector(cons.f3).toString());
	}

	@Test
	public void testLinearProgramming() {
		double[] objData = new double[] {2, 3};
		LinearObjectiveFunction objFunc = new LinearObjectiveFunction(objData, 0);
		List <LinearConstraint> cons = new ArrayList <>();
		cons.add(new LinearConstraint(new double[] {1, 1}, Relationship.LEQ, 8));
		cons.add(new LinearConstraint(new double[] {0, 1}, Relationship.LEQ, 2));
		cons.add(new LinearConstraint(new double[] {1, 0}, Relationship.GEQ, 4));
		cons.add(new LinearConstraint(new double[] {1, 0}, Relationship.GEQ, 0));
		cons.add(new LinearConstraint(new double[] {0, 1}, Relationship.GEQ, 0));
		LinearConstraintSet conSet = new LinearConstraintSet(cons);
		PointValuePair pair = new SimplexSolver().optimize(objFunc, conSet, GoalType.MAXIMIZE);
		for (double v : pair.getPoint()) {
			System.out.println(v);
		}
	}

	@Test
	public void testConsBetweenFeatures() {
		ConstraintBetweenFeatures cons = new ConstraintBetweenFeatures();
		cons.addScale(1, 2, 2.);
		cons.addEqual(3, 2.);
		cons.addLessThan(1, 1.);
		cons.addLargerThan(4, 2.3);
		cons.addLessThanFeature(3, 4);
		cons.addLargerThanFeature(4, 5);
		String s = cons.extractConstraint(new int[] {1, 2, 3, 4, 5}).toString();
		ConstraintBetweenFeatures.fromJson(s);
		ConstraintBetweenFeatures.fromJson("");
		cons = new ConstraintBetweenFeatures();
		cons.addEqual(1, 2.);
		cons.addLargerThanFeature(2, 3);

		ConstraintBetweenFeatures.fromJson(cons.toString());
		cons = new ConstraintBetweenFeatures();
		cons.addLargerThanFeature("f0", 0, "f1", 0);
		cons.addLargerThanFeature("f2", 0, "f3", 0);
		ConstraintBetweenFeatures.fromJson(cons.toString());
	}
}