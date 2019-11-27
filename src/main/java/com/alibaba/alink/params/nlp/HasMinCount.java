package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Minimum count of a word.
 */
public interface HasMinCount<T> extends WithParams<T> {
	ParamInfo <Integer> MIN_COUNT = ParamInfoFactory
		.createParamInfo("minCount", Integer.class)
		.setDescription("minimum count of word")
		.setHasDefaultValue(5)
		.build();

	default Integer getMinCount() {
		return get(MIN_COUNT);
	}

	default T setMinCount(Integer value) {
		return set(MIN_COUNT, value);
	}
}
