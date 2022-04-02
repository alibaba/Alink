package com.alibaba.alink.params.utils;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface SideOutputParams<T> extends WithParams <T> {
	ParamInfo <Integer> INDEX = ParamInfoFactory
		.createParamInfo("index", Integer.class)
		.setDescription("index of side outputs")
		.setHasDefaultValue(0)
		.build();

	default Integer getIndex() {
		return get(INDEX);
	}

	default T setIndex(Integer index) {
		return set(INDEX, index);
	}
}
