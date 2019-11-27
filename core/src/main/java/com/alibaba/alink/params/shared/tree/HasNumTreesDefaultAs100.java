package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumTreesDefaultAs100<T> extends WithParams<T> {
	ParamInfo <Integer> NUM_TREES = ParamInfoFactory
		.createParamInfo("numTrees", Integer.class)
		.setDescription("Number of decision trees.")
		.setHasDefaultValue(100)
		.setAlias(new String[] {"treeNum"})
		.build();

	default Integer getNumTrees() {
		return get(NUM_TREES);
	}

	default T setNumTrees(Integer value) {
		return set(NUM_TREES, value);
	}
}
