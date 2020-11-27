package com.alibaba.alink.operator.common.tree.parallelcart.loss;

public interface RankingLossFunc {
	void gradients(
		int[] queryOffset,
		int queryId,
		int numIter,
		double[] y,
		double[] eta,
		double[] weights,
		double[] gradients,
		double[] hessions);
}
