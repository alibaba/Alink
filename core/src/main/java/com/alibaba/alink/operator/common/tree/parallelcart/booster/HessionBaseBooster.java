package com.alibaba.alink.operator.common.tree.parallelcart.booster;

import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;
import com.alibaba.alink.operator.common.tree.parallelcart.BoostingObjs;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;

public class HessionBaseBooster implements Booster {
	final UnaryLossFunc loss;
	final Slice slice;
	final double[] gradient;
	final double[] hession;
	final double[] weights;

	public HessionBaseBooster(
		UnaryLossFunc loss,
		double[] weights,
		Slice slice) {
		this.loss = loss;
		this.weights = weights;
		this.slice = slice;

		this.gradient = new double[slice.end - slice.start];
		this.hession = new double[slice.end - slice.start];
	}

	@Override
	public void boosting(
		BoostingObjs boostingObjs,
		double[] label,
		double[] pred) {
		for (int i = slice.start; i < slice.end; ++i) {
			double eta = pred[i];
			double y = label[i];
			gradient[i] = loss.derivative(eta, y);
			hession[i] = loss.secondDerivative(eta, y);
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
		return hession;
	}

	@Override
	public double[] getGradientsSqr() {
		return null;
	}
}
