package com.alibaba.alink.operator.common.tree.parallelcart.leafscoreupdater;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public enum LeafScoreUpdaterType {
	NEWTON_SINGLE_STEP_UPDATER,
	WEIGHT_AVG_UPDATER;

	public static final ParamInfo <LeafScoreUpdaterType> LEAF_SCORE_UPDATER_TYPE = ParamInfoFactory
		.createParamInfo("Type of leaf score updater", LeafScoreUpdaterType.class)
		.build();
}
