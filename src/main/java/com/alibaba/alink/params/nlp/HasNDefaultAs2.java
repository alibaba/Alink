package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params n, the length of ngram.
 */
public interface HasNDefaultAs2<T> extends WithParams<T> {
	ParamInfo <Integer> N = ParamInfoFactory
		.createParamInfo("n", Integer.class)
		.setDescription("NGram length")
		.setHasDefaultValue(2)
		.build();

	default Integer getN() {
		return get(N);
	}

	default T setN(Integer value) {
		return set(N, value);
	}
}
