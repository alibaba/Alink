package com.alibaba.alink.operator.common.optim.activeSet;

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
import java.util.Arrays;
import java.util.List;

public class SimplexTest extends AlinkTestBase {
	@Test
	public void test() {
		//3*x+4*y
		LinearObjectiveFunction obj = new LinearObjectiveFunction(new double[] {1, 1}, 0);
		LinearConstraint cons1 = new LinearConstraint(new double[] {1, 2}, Relationship.LEQ, 9);
		LinearConstraint cons2 = new LinearConstraint(new double[] {2, 1}, Relationship.LEQ, 12);
		LinearConstraint cons3 = new LinearConstraint(new double[] {0, 1}, Relationship.GEQ, 0);
		LinearConstraint cons4 = new LinearConstraint(new double[] {1, 0}, Relationship.GEQ, 0);
		List <LinearConstraint> cons = new ArrayList <>();
		cons.add(cons1);
		cons.add(cons2);
		cons.add(cons3);
		cons.add(cons4);
		LinearConstraintSet conSet = new LinearConstraintSet(cons);
		PointValuePair pair = new SimplexSolver().optimize(obj, conSet, GoalType.MAXIMIZE);
		System.out.println(Arrays.toString(pair.getPoint()));
	}
}
