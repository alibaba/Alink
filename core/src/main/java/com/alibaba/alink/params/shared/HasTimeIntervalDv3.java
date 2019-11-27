package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: Time interval of streaming windows, unit s.
 */
public interface HasTimeIntervalDv3<T> extends WithParams<T> {

	ParamInfo <Integer> TIME_INTERVAL = ParamInfoFactory
		.createParamInfo("timeInterval", Integer.class)
		.setDescription("Time interval of streaming windows, unit s.")
		.setHasDefaultValue(3)
		.build();

	default Integer getTimeInterval() {
		return get(TIME_INTERVAL);
	}

	default T setTimeInterval(Integer value) {
		return set(TIME_INTERVAL, value);
	}

}
