package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Perceptron loss function.
 */
public class PerceptronLossFunc implements UnaryLossFunc {
	public PerceptronLossFunc() { }

	@Override
	public double loss(double eta, double y) {
		return Math.max(0, -eta * y);
	}

	@Override
	public double derivative(double eta, double y) {
		return eta * y < 0 ? -y : 0;
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return 0;
	}
}
