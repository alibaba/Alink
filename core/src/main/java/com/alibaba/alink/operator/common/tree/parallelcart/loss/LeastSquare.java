package com.alibaba.alink.operator.common.tree.parallelcart.loss;

public class LeastSquare implements UnaryLossFuncWithPrior {
	private static final long serialVersionUID = -2732060307610961883L;

	private final double mean;

	public LeastSquare(double sum, double weightSum) {
		this.mean = sum / weightSum;
	}

	@Override
	public double prior() {
		return mean;
	}

	@Override
	public double loss(double eta, double y) {
		return Math.pow(y - eta, 2.0);
	}

	@Override
	public double derivative(double eta, double y) {
		return y - eta;
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return 1.0;
	}
}
