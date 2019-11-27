package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasUrl<T> extends WithParams<T> {
	ParamInfo <String> URL = ParamInfoFactory
		.createParamInfo("url", String.class)
		.setDescription("url")
		.setRequired()
		.build();

	default String getUrl() {
		return get(URL);
	}

	default T setUrl(String value) {
		return set(URL, value);
	}
}
