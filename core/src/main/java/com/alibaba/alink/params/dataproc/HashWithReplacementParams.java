package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HashWithReplacementParams<T> extends WithParams <T> {

	/**
	 * @cn-name 是否放回
	 * @cn 是否有放回的采样，默认不放回
	 */
	ParamInfo <Boolean> WITH_REPLACEMENT = ParamInfoFactory
		.createParamInfo("withReplacement", Boolean.class)
		.setDescription("Indicates whether to enable sampling with replacement, default is without replacement")
		.setHasDefaultValue(false)
		.build();

	default Boolean getWithReplacement() {
		return getParams().get(WITH_REPLACEMENT);
	}

	default T setWithReplacement(Boolean value) {
		return set(WITH_REPLACEMENT, value);
	}
}
