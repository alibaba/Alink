package com.alibaba.alink.operator.common.tree.parallelcart.booster;

import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;
import com.alibaba.alink.operator.common.tree.parallelcart.BoostingObjs;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;

public class GradientBaseBooster implements Booster {
	UnaryLossFunc loss;
	Slice slice;
	double[] gradient;
	double[] gradientSqr;
	double[] weights;

	public GradientBaseBooster(
		UnaryLossFunc loss,
		double[] weights,
		Slice slice) {
		this.loss = loss;
		this.slice = slice;

		this.weights = weights;
		this.gradient = new double[this.slice.end - this.slice.start];
		this.gradientSqr = new double[this.slice.end - this.slice.start];
	}

	@Override
	public void boosting(
		BoostingObjs boostingObjs,
		double[] label,
		double[] pred
	) {
		for (int i = slice.start; i < slice.end; ++i) {
			double eta = pred[i];
			double y = label[i];
			double derivative = loss.derivative(eta, y);
			gradient[i] = derivative;
			gradientSqr[i] = derivative * derivative;
		}
	}

	@Override
	public double[] getWeights() {
		return weights;
	}

	@Override
	public double[] getGradients() {
		return gradient;
	}

	@Override
	public double[] getHessions() {
		return null;
	}

	@Override
	public double[] getGradientsSqr() {
		return gradientSqr;
	}
}
