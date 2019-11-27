package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasStartTime_null<T> extends WithParams<T> {
	ParamInfo <String> START_TIME = ParamInfoFactory
		.createParamInfo("startTime", String.class)
		.setDescription("start time")
		.setHasDefaultValue(null)
		.build();

	default String getStartTime() {return get(START_TIME);}

	default T setStartTime(String value) {return set(START_TIME, value);}
}
