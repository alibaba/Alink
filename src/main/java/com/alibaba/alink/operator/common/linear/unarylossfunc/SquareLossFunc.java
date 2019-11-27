package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Squared loss function.
 * https://en.wikipedia.org/wiki/Loss_functions_for_classification#Square_loss
 */
public class SquareLossFunc implements UnaryLossFunc {
	public SquareLossFunc() { }

	@Override
	public double loss(double eta, double y) {
		return 0.5 * (eta - y) * (eta - y);
	}

	@Override
	public double derivative(double eta, double y) {
		return eta - y;
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return 1;
	}
}
