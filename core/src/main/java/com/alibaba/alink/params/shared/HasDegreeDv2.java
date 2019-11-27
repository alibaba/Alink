package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDegreeDv2<T> extends WithParams<T> {
	ParamInfo <Integer> DEGREE = ParamInfoFactory
		.createParamInfo("degree", Integer.class)
		.setDescription("degree of polynomial expand.")
		.setHasDefaultValue(2)
		.build();

	default Integer getDegree() {
		return get(DEGREE);
	}

	default T setDegree(Integer value) {
		return set(DEGREE, value);
	}
}
