package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasUserParams<T> extends WithParams <T> {

	ParamInfo <String> USER_PARAMS = ParamInfoFactory
		.createParamInfo("userParams", String.class)
		.setDescription("params from user, in json format")
		.setHasDefaultValue("{}")
		.build();

	default String getUserParams() {
		return get(USER_PARAMS);
	}

	default T setUserParams(String value) {
		return set(USER_PARAMS, value);
	}
}
