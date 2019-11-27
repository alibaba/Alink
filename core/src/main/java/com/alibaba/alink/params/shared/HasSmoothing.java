package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param of the smoothing factor.
 */
public interface HasSmoothing<T> extends WithParams<T> {

	ParamInfo <Double> SMOOTHING = ParamInfoFactory
		.createParamInfo("smoothing", Double.class)
		.setDescription("the smoothing factor")
		.setHasDefaultValue(1.0)
		.build();

	default Double getSmoothing() {
		return get(SMOOTHING);
	}

	default T setSmoothing(Double value) {
		return set(SMOOTHING, value);
	}
}
