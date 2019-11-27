package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTargetType<T> extends WithParams<T> {

	ParamInfo <String> TARGET_TYPE = ParamInfoFactory
		.createParamInfo("targetType", String.class)
		.setDescription("The target type of numerical column cast function.")
		.setRequired()
		.setAlias(new String[] {"newType"})
		.build();

	default String getTargetType() {
		return get(TARGET_TYPE);
	}

	default T setTargetType(String value) {
		return set(TARGET_TYPE, value);
	}
}
