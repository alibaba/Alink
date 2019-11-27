package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Whether has intercept or not, default is true.
 *
 */
public interface HasWithIntercept<T> extends WithParams<T> {

	ParamInfo <Boolean> WITH_INTERCEPT = ParamInfoFactory
		.createParamInfo("withIntercept", Boolean.class)
		.setDescription("Whether has intercept or not, default is true")
		.setAlias(new String[]{"hasInterceptItem"})
		.setHasDefaultValue(true)
		.build();

	default Boolean getWithIntercept() {
		return get(WITH_INTERCEPT);
	}

	default T setWithIntercept(Boolean value) {
		return set(WITH_INTERCEPT, value);
	}
}
