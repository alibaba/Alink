package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTrendStart<T> extends WithParams <T> {

	/**
	 * @cn-name trend初始值
	 * @cn trend初始值
	 */
	ParamInfo <Double> TREND_START = ParamInfoFactory
		.createParamInfo("trendStart", Double.class)
		.setDescription("The trend start.")
		.build();

	default Double getTrendStart() {
		return get(TREND_START);
	}

	default T setTrendStart(Double value) {
		return set(TREND_START, value);
	}

}
