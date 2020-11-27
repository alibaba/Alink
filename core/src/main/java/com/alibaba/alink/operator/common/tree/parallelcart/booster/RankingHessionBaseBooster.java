package com.alibaba.alink.operator.common.tree.parallelcart.booster;

import com.alibaba.alink.operator.common.tree.parallelcart.BoostingObjs;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.RankingLossFunc;

public class RankingHessionBaseBooster implements Booster {
	final RankingLossFunc loss;
	final int[] queryOffset;
	final Slice queryIdOffsetSlice;
	final Slice dataSlice;
	final double[] gradient;
	final double[] hession;
	final double[] weights;

	public RankingHessionBaseBooster(
		RankingLossFunc loss,
		int[] queryOffset,
		double[] weights,
		Slice queryIdOffsetSlice,
		Slice dataSlice) {
		this.loss = loss;
		this.queryOffset = queryOffset;
		this.weights = weights;
		this.queryIdOffsetSlice = queryIdOffsetSlice;
		this.dataSlice = dataSlice;

		this.gradient = new double[dataSlice.end - dataSlice.start];
		this.hession = new double[dataSlice.end - dataSlice.start];
	}

	public void boosting(
		BoostingObjs boostingObjs,
		double[] label,
		double[] pred) {
		for (int i = queryIdOffsetSlice.start; i < queryIdOffsetSlice.end; ++i) {
			loss.gradients(queryOffset, i, boostingObjs.numBoosting, label, pred, weights, gradient, hession);
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
