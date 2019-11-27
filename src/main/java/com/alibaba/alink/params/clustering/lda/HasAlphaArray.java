package com.alibaba.alink.params.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * doc parameter array.
 */
public interface HasAlphaArray<T> extends WithParams<T> {
	ParamInfo <Double[]> ALPHA_ARRAY = ParamInfoFactory
		.createParamInfo("alphaArray", Double[].class)
		.setDescription(
			"alpha.Concentration parameter (commonly named \"alpha\") for the prior placed on documents' distributions"
				+ " over topics (\"beta\").")
		.setHasDefaultValue(null)
		.build();

	default Double[] getAlphaArray() {
		return get(ALPHA_ARRAY);
	}

	default T setAlphaArray(Double[] value) {
		return set(ALPHA_ARRAY, value);
	}
}
