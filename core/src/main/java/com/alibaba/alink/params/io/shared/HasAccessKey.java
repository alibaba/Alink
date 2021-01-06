package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasAccessKey<T> extends WithParams <T> {

	ParamInfo <String> ACCESS_KEY = ParamInfoFactory
		.createParamInfo("accessKey", String.class)
		.setDescription("access key")
		.setRequired()
		.build();

	default String getAccessKey() {
		return get(ACCESS_KEY);
	}

	default T setAccessKey(String value) {
		return set(ACCESS_KEY, value);
	}
}
