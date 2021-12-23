package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumTreesOfInfoGain<T> extends WithParams <T> {
	/**
	 * @cn-name 模型中Id3树的棵数
	 * @cn 模型中Id3树的棵数
	 */
	ParamInfo <Integer> NUM_TREES_OF_INFO_GAIN = ParamInfoFactory
		.createParamInfo("numTreesOfInfoGain", Integer.class)
		.setDescription("Number of id3 trees.")
		.setHasDefaultValue(null)
		.build();

	default Integer getNumTreesOfInfoGain() {
		return get(NUM_TREES_OF_INFO_GAIN);
	}

	default T setNumTreesOfInfoGain(Integer value) {
		return set(NUM_TREES_OF_INFO_GAIN, value);
	}
}
