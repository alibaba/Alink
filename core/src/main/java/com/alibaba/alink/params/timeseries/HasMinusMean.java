package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinusMean<T> extends WithParams <T> {

	/**
	 * @cn-name 是否减去均值
	 * @cn 是否减去均值
	 */
	ParamInfo <Boolean> MINUS_MEAN = ParamInfoFactory
		.createParamInfo("minusMean", Boolean.class)
		.setDescription("minusMean")
		.setHasDefaultValue(true)
		.build();

	default Boolean getMinusMean() {
		return get(MINUS_MEAN);
	}

	default T setMinusMean(Boolean value) {
		return set(MINUS_MEAN, value);
	}
}
