package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumTreesOfInfoGainRatio<T> extends WithParams <T> {
	/**
	 * @cn-name 模型中C4.5树的棵数
	 * @cn 模型中C4.5树的棵数
	 */
	ParamInfo <Integer> NUM_TREES_OF_INFO_GAIN_RATIO = ParamInfoFactory
		.createParamInfo("numTreesOfInfoGainRatio", Integer.class)
		.setDescription("Number of c4.5 trees.")
		.setHasDefaultValue(null)
		.build();

	default Integer getNumTreesOfInfoGainRatio() {
		return get(NUM_TREES_OF_INFO_GAIN_RATIO);
	}

	default T setNumTreesOfInfoGainRatio(Integer value) {
		return set(NUM_TREES_OF_INFO_GAIN_RATIO, value);
	}
}
