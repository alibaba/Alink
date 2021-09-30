package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface ProphetParams<T> extends
	TimeSeriesPredictParams <T> {

	ParamInfo <Integer> UNCERTAINTY_SAMPLES = ParamInfoFactory
		.createParamInfo("uncertaintySamples", Integer.class)
		.setDescription("uncertainty_samples")
		.setHasDefaultValue(1000)
		.build();

	default Integer getUncertaintySamples() {
		return get(UNCERTAINTY_SAMPLES);
	}

	default T setUncertaintySamples(Integer value) {
		return set(UNCERTAINTY_SAMPLES, value);
	}

	ParamInfo <String> STAN_INIT = ParamInfoFactory
		.createParamInfo("stanInit", String.class)
		.setDescription("stan_init")
		.setHasDefaultValue(null)
		.build();

	default String getStanInit() {
		return get(STAN_INIT);
	}

	default T setStanInit(String value) {
		return set(STAN_INIT, value);
	}
}
