package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSeasonalStart<T> extends WithParams <T> {

	/**
	 * @cn-name seasonal初始值
	 * @cn seasonal初始值
	 */
	ParamInfo <double[]> SEASONAL_START = ParamInfoFactory
		.createParamInfo("seasonalStart", double[].class)
		.setDescription("The seasonal start.")
		.build();

	default double[] getSeasonalStart() {
		return get(SEASONAL_START);
	}

	default T setSeasonalStart(double... value) {
		return set(SEASONAL_START, value);
	}

}
