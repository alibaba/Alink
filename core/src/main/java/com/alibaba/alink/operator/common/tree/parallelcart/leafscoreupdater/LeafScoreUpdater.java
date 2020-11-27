package com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater;

public interface LeafScoreUpdater {
	void update(double weight, double[] distribution, double learningRate);
}
