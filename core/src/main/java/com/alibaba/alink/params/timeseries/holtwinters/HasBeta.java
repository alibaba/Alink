package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasBeta<T> extends WithParams <T> {


	/**
	 * @cn-name beta
	 * @cn beta
	 */
	ParamInfo <Double> BETA = ParamInfoFactory
		.createParamInfo("beta", Double.class)
		.setDescription("The beta.")
		.setHasDefaultValue(0.1)
		.setValidator(HasAlpha.validator)
		.build();

	default Double getBeta() {
		return get(BETA);
	}

	default T setBeta(Double value) {
		return set(BETA, value);
	}
}
