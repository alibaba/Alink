package com.alibaba.alink.operator.common.tree.parallelcart.booster;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public enum BoosterType {
	GRADIENT_BASE,
	HESSION_BASE;

	public static final ParamInfo <BoosterType> BOOSTER_TYPE = ParamInfoFactory
		.createParamInfo("boosterType", BoosterType.class)
		.build();
}
