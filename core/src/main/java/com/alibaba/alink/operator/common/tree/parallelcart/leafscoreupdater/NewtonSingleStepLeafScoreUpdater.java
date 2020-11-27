package com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater;

public class NewtonSingleStepLeafScoreUpdater implements LeafScoreUpdater {
	private final static double HESSION_EPS = 0.00001;

	@Override
	public void update(double weight, double[] distribution, double learningRate) {
		distribution[0] *= learningRate;
		distribution[0] /= Math.max(HESSION_EPS, distribution[1]);
	}
}
