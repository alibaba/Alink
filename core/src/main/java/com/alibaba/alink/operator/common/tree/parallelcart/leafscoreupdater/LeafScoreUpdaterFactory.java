package com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater;

public class LeafScoreUpdaterFactory {
	public static LeafScoreUpdater createLeafScoreUpdater(LeafScoreUpdaterType type) {
		switch (type) {
			case NEWTON_SINGLE_STEP_UPDATER:
				return new NewtonSingleStepLeafScoreUpdater();
			case WEIGHT_AVG_UPDATER:
				return new WeightAvgLeafScoreUpdater();
			default:
				throw new UnsupportedOperationException("Unsupported.");
		}
	}
}
