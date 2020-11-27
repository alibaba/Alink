package com.alibaba.alink.operator.common.tree.parallelcart.loss;

public class LogLoss implements UnaryLossFuncWithPrior {
	private static final long serialVersionUID = 4297320979230510859L;
	private final double positive;
	private final double negative;

	public LogLoss(double positive, double negative) {
		this.positive = positive;
		this.negative = negative;
	}

	@Override
	public double prior() {
		return Math.log(positive / negative);
	}

	@Override
	public double loss(double eta, double y) {
		return -y * eta + Math.log(1 + Math.exp(eta));
	}

	@Override
	public double derivative(double eta, double y) {
		return y - LambdaLoss.sigmoid(eta);
	}

	@Override
	public double secondDerivative(double eta, double y) {
		double derivative = derivative(eta, y);
		return (y - derivative) * (1 - y + derivative);
	}
}
