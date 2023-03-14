package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTimeWindow<T> extends WithParams <T> {
	ParamInfo <Double> TIME_WINDOW = ParamInfoFactory
		.createParamInfo("timeWindow", Double.class)
		.setDescription("the time window")
		.setRequired()
		.build();

	default Double getTimeWindow() {
		return get(TIME_WINDOW);
	}

	default T setTimeWindow(Double value) {
		return set(TIME_WINDOW, value);
	}
}
