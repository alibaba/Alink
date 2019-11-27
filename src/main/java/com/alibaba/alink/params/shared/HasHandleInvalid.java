package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * method to deal with invalid situation.
 */
public interface HasHandleInvalid<T> extends WithParams<T> {
	ParamInfo <String> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalidMethod", String.class)
		.setDescription("the handle method of invalid value. include： error, optimistic")
		.setHasDefaultValue("error")
		.build();

	default String getHandleInvalid() {
		return get(HANDLE_INVALID);
	}

	default T setHandleInvalid(String value) {
		return set(HANDLE_INVALID, value);
	}
}
