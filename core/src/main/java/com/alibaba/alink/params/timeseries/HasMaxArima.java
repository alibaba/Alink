package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMaxArima<T> extends WithParams <T> {

	/**
	 * @cn-name 最大arima阶数
	 * @cn 最大arima阶数
	 */
	ParamInfo <Integer> MAX_ARIMA = ParamInfoFactory
		.createParamInfo("maxARIMA", Integer.class)
		.setDescription("max arima")
		.setHasDefaultValue(10)
		.build();

	default Integer getMaxARIMA() {
		return get(MAX_ARIMA);
	}

	default T setMaxARIMA(Integer value) {
		return set(MAX_ARIMA, value);
	}
}
