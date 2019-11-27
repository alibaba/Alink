package com.alibaba.alink.operator.common.linear.unarylossfunc;

/**
 * Zero-one loss function.
 */
public class ZeroOneLossFunc implements UnaryLossFunc {
	public ZeroOneLossFunc() { }

	@Override
	public double loss(double eta, double y) {
		return eta * y < 0 ? 1.0 : 0.0;
	}

	@Override
	public double derivative(double eta, double y) {
		return 0;
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return 0;
	}
}
