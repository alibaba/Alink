package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPackagePath<T> extends WithParams<T> {
	ParamInfo <String> PACKAGE_PATH = ParamInfoFactory
		.createParamInfo("packagePath", String.class)
		.setDescription("package path for invovation.")
		.setRequired()
		.build();

	default String getPackagePath() {
		return get(PACKAGE_PATH);
	}

	default T setPackagePath(String value) {
		return set(PACKAGE_PATH, value);
	}
}
