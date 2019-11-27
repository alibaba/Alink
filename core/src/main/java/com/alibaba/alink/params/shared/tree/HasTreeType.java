package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTreeType<T> extends WithParams<T> {
	ParamInfo <String> TREE_TYPE = ParamInfoFactory
		.createParamInfo("treeType", String.class)
		.setDescription("treeType")
		.setHasDefaultValue("avg")
		.build();

	default String getTreeType() {
		return get(TREE_TYPE);
	}

	default T setTreeType(String value) {
		return set(TREE_TYPE, value);
	}
}
