package com.alibaba.alink.params.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * word parameter array.
 */
public interface HasBetaArray<T> extends WithParams<T> {
	ParamInfo <Double[]> BETA_ARRAY = ParamInfoFactory
		.createParamInfo("betaArray", Double[].class)
		.setDescription(
			"Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topics' "
				+ "distributions over terms.")
		.setHasDefaultValue(null)
		.build();

	default Double[] getBetaArray() {
		return get(BETA_ARRAY);
	}

	default T setBetaArray(Double[] value) {
		return set(BETA_ARRAY, value);
	}

}
