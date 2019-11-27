package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxLeaves<T> extends WithParams<T> {
	ParamInfo <Integer> MAX_LEAVES = ParamInfoFactory
		.createParamInfo("maxLeaves", Integer.class)
		.setDescription("max leaves of tree")
		.setHasDefaultValue(Integer.MAX_VALUE)
		.setAlias(new String[] {"bestLeafs"})
		.build();

	default Integer getMaxLeaves() {
		return get(MAX_LEAVES);
	}

	default T setMaxLeaves(Integer value) {
		return set(MAX_LEAVES, value);
	}
}
