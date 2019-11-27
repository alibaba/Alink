package com.alibaba.alink.operator.common.linear.unarylossfunc;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for the unary loss functions.
 */
public class UnaryLossFuncTest {
	@Test
	public void test() throws Exception {
		UnaryLossFunc[] lossFuncs;

		lossFuncs = new UnaryLossFunc[] {
			new ExponentialLossFunc(),
			new HingeLossFunc(),
			new LogisticLossFunc(),
			new LogLossFunc(),
			new PerceptronLossFunc(),
			new ZeroOneLossFunc()
		};

		for (UnaryLossFunc lossFunc : lossFuncs) {
			assertTrue(lossFunc.loss(-1.0, 1.0) > 1.0 - 1e-10);
			assertTrue(lossFunc.loss(1.0, 1.0) < 0.5);
			assertTrue(lossFunc.loss(-0.5, 1.0) - lossFunc.loss(0.5, 1.0) > 0.49);
			assertTrue(lossFunc.loss(-0.5, 1.0) == lossFunc.loss(0.5, -1.0));
			assertTrue(lossFunc.derivative(-0.5, 1.0) <= lossFunc.derivative(0.5, 1.0));
			assertTrue(lossFunc.secondDerivative(-0.5, 1.0) >= lossFunc.secondDerivative(0.5, 1.0));
		}

		lossFuncs = new UnaryLossFunc[] {
			new SquareLossFunc(),
			new SvrLossFunc(1.0),
			new HuberLossFunc(1.0)
		};

		for (UnaryLossFunc lossFunc : lossFuncs) {
			assertTrue(Math.abs(lossFunc.loss(0.0, 0.0)) < 1e-10);
			assertTrue(Math.abs(lossFunc.derivative(0.0, 0.0)) < 1e-10);
			assertTrue(lossFunc.derivative(-0.5, 0.0) == -lossFunc.derivative(0.5, 0.0));
			assertTrue(lossFunc.secondDerivative(-0.5, 0.0) == lossFunc.secondDerivative(0.5, 0.0));
		}
	}
}