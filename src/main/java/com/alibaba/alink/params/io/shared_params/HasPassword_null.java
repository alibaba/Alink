package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPassword_null<T> extends WithParams<T> {
	ParamInfo <String> PASSWORD = ParamInfoFactory
		.createParamInfo("password", String.class)
		.setDescription("password")
		.setHasDefaultValue(null)
		.build();

	default String getPassword() {
		return get(PASSWORD);
	}

	default T setPassword(String value) {
		return set(PASSWORD, value);
	}
}
