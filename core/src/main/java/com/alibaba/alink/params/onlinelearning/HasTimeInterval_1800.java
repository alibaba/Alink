package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTimeInterval_1800<T> extends WithParams<T> {
	ParamInfo <Integer> TIME_INTERVAL = ParamInfoFactory
		.createParamInfo("timeInterval", Integer.class)
		.setDescription("time interval")
		.setHasDefaultValue(1800)
		.build();

	default Integer getTimeInterval() {return get(TIME_INTERVAL);}

	default T setTimeInterval(Integer value) {return set(TIME_INTERVAL, value);}
}
