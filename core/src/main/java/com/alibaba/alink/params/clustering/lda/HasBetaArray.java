package com.alibaba.alink.params.clustering.lda;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * word parameter array.
 */
public interface HasBetaArray<T> extends WithParams <T> {
	/**
	 * @cn 词的超参
	 */
	ParamInfo <double[]> BETA_ARRAY = ParamInfoFactory
		.createParamInfo("betaArray", double[].class)
		.setDescription(
			"Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topics' "
				+ "distributions over terms.")
		.setHasDefaultValue(null)
		.build();

	default double[] getBetaArray() {
		return get(BETA_ARRAY);
	}

	default T setBetaArray(double[] value) {
		return set(BETA_ARRAY, value);
	}

}
