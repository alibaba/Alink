package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumTreesOfGini<T> extends WithParams <T> {
	/**
	 * @cn-name 模型中Cart树的棵数
	 * @cn 模型中Cart树的棵数
	 */
	ParamInfo <Integer> NUM_TREES_OF_GINI = ParamInfoFactory
		.createParamInfo("numTreesOfGini", Integer.class)
		.setDescription("Number of cart trees.")
		.setHasDefaultValue(null)
		.build();

	default Integer getNumTreesOfGini() {
		return get(NUM_TREES_OF_GINI);
	}

	default T setNumTreesOfGini(Integer value) {
		return set(NUM_TREES_OF_GINI, value);
	}
}
