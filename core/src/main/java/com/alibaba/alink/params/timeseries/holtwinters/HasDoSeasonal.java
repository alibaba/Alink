package com.alibaba.alink.params.timeseries.holtwinters;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDoSeasonal<T> extends WithParams <T> {
	ParamInfo <Boolean> DO_SEASONAL = ParamInfoFactory
		.createParamInfo("doSeasonal", Boolean.class)
		.setDescription("Whether time serial has seasonal or not.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getDoSeasonal() {
		return get(DO_SEASONAL);
	}

	default T setDoSeasonal(Boolean value) {
		return set(DO_SEASONAL, value);
	}
}
