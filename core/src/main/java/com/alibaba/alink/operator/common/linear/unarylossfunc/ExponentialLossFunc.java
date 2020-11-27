package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Exponential loss function.
 * https://en.wikipedia.org/wiki/Loss_functions_for_classification#Exponential_loss
 */
public class ExponentialLossFunc implements UnaryLossFunc {
	private static final long serialVersionUID = -7976736283876322615L;

	public ExponentialLossFunc() { }

	@Override
	public double loss(double eta, double y) {
		return Math.exp(-eta * y);
	}

	@Override
	public double derivative(double eta, double y) {
		return -y * Math.exp(-eta * y);
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return y * y * Math.exp(-eta * y);
	}
}
