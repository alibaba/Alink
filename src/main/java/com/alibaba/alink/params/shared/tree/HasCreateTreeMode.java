package com.alibaba.alink.params.shared.tree;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasCreateTreeMode<T> extends WithParams<T> {
	ParamInfo<String> CREATE_TREE_MODE = ParamInfoFactory
		.createParamInfo("createTreeMode", String.class)
		.setDescription("series or parallel")
		.setHasDefaultValue("series")
		.build();

	default String getCreateTreeMode() {
		return get(CREATE_TREE_MODE);
	}

	default T setCreateTreeMode(String value) {
		return set(CREATE_TREE_MODE, value);
	}
}
