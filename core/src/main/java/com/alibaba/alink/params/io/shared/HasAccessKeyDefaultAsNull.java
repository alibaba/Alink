package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasAccessKeyDefaultAsNull<T> extends WithParams <T> {
	/**
	 * @cn-name accessKey
	 * @cn accessKey
	 */
	ParamInfo <String> ACCESS_KEY = ParamInfoFactory
		.createParamInfo("accessKey", String.class)
		.setDescription("access key")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"accesskey"})
		.build();

	default String getAccessKey() {
		return get(ACCESS_KEY);
	}

	default T setAccessKey(String value) {
		return set(ACCESS_KEY, value);
	}
}
