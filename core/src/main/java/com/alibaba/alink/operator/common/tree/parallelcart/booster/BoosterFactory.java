package com.alibaba.alink.operator.common.tree.parallelcart.booster;

import com.alibaba.alink.operator.common.linear.unarylossfunc.UnaryLossFunc;
import com.alibaba.alink.operator.common.tree.parallelcart.data.Slice;
import com.alibaba.alink.operator.common.tree.parallelcart.loss.RankingLossFunc;

public class BoosterFactory {
	public static Booster createBooster(
		BoosterType type,
		UnaryLossFunc loss,
		double[] weights,
		Slice slice) {
		switch (type) {
			case HESSION_BASE:
				return new HessionBaseBooster(loss, weights, slice);
			case GRADIENT_BASE:
				return new GradientBaseBooster(loss, weights, slice);
			default:
				throw new UnsupportedOperationException("Unsupported.");
		}
	}

	public static Booster createRankingBooster(
		BoosterType type,
		RankingLossFunc loss,
		int[] queryOffset,
		double[] weights,
		Slice queryIdOffsetSlice,
		Slice dataSlice) {
		switch (type) {
			case HESSION_BASE:
				return new RankingHessionBaseBooster(loss, queryOffset, weights, queryIdOffsetSlice, dataSlice);
			case GRADIENT_BASE:
			default:
				throw new UnsupportedOperationException("Unsupported.");
		}
	}
}
