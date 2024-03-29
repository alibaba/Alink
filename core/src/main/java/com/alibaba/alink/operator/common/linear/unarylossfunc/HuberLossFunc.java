package com.alibaba.alink.operator.common.linear.unarylossfunc;

import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;

/**
 * Huber loss function.
 * https://en.wikipedia.org/wiki/Huber_loss
 */
public class HuberLossFunc implements UnaryLossFunc {
	private static final long serialVersionUID = 618967763754853703L;
	private final double delta;

	public HuberLossFunc(double delta) {
		if (delta <= 0) {
			throw new AkIllegalArgumentException("Parameter delta must be positive.");
		}
		this.delta = delta;
	}

	@Override
	public double loss(double eta, double y) {
		double x = Math.abs(eta - y);
		return x > delta ? delta * (x - delta / 2) : x * x / 2;
	}

	@Override
	public double derivative(double eta, double y) {
		double x = eta - y;
		return Math.abs(x) > delta ? Math.signum(x) * delta : x;
	}

	@Override
	public double secondDerivative(double eta, double y) {
		return Math.abs(eta - y) > delta ? 0.0 : 1.0;
	}
}
