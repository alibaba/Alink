package com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater;

public class WeightAvgLeafScoreUpdater implements LeafScoreUpdater {

	@Override
	public void update(double weight, double[] distribution, double learningRate) {
		distribution[0] *= learningRate;
		distribution[0] /= weight;
	}
}
