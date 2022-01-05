package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.dl.HasPythonEnv;

public interface ProphetParams<T> extends
	TimeSeriesPredictParams <T> , HasPythonEnv <T> {

	/**
	 * @cn-name 用来计算指标的采样数目
	 * @cn 用来计算指标的采样数目，设置成0，不计算指标。
	 */
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

	/**
	 * @cn-name 初始值
	 * @cn 初始值
	 */
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
