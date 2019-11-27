package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: binary. If true, all non-zero counts are set to 1.
 *
 */
public interface HasBinary<T> extends WithParams<T> {
	ParamInfo <Boolean> BINARY = ParamInfoFactory
		.createParamInfo("binary", Boolean.class)
		.setDescription("If true, all non-zero counts are set to 1.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getBinary() {
		return get(BINARY);
	}

	default T setBinary(Boolean value) {
		return set(BINARY, value);
	}

}
