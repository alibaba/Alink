package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinConfidenceDefaultAs005<T> extends WithParams<T> {
	ParamInfo <Double> MIN_CONFIDENCE = ParamInfoFactory
		.createParamInfo("minConfidence", Double.class)
		.setDescription("Minimum confidence")
		.setHasDefaultValue(0.05)
		.build();

	default Double getMinConfidence() {
		return get(MIN_CONFIDENCE);
	}

	default T setMinConfidence(Double value) {
		return set(MIN_CONFIDENCE, value);
	}
}
