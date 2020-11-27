package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTimeInterval<T> extends WithParams <T> {

	ParamInfo <Long> TIME_INTERVAL = ParamInfoFactory
		.createParamInfo("timeInterval", Long.class)
		.setDescription("time interval, unit is s.")
		.setRequired()
		.build();

	default Long getTimeInterval() {
		return get(TIME_INTERVAL);
	}

	default T setTimeInterval(Long value) {
		return set(TIME_INTERVAL, value);
	}

}
