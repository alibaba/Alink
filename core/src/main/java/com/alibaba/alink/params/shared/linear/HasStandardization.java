package com.alibaba.alink.params.shared.linear;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Whether standardize training data or not, default is true.
 *
 */
public interface HasStandardization<T> extends WithParams<T> {
	ParamInfo <Boolean> STANDARDIZATION = ParamInfoFactory
		.createParamInfo("standardization", Boolean.class)
		.setDescription("Whether standardize training data or not, default is true")
		.setHasDefaultValue(true)
		.build();

	default Boolean getStandardization() {
		return get(STANDARDIZATION);
	}

	default T setStandardization(Boolean value) {
		return set(STANDARDIZATION, value);
	}
}
