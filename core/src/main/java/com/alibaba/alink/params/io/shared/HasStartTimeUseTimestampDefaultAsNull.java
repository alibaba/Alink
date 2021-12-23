package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasStartTimeUseTimestampDefaultAsNull<T> extends WithParams <T> {
	/**
	 * @cn-name 起始时间
	 * @cn 起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s)
	 */
	ParamInfo <String> START_TIME = ParamInfoFactory
		.createParamInfo("startTime", String.class)
		.setDescription("start time")
		.setHasDefaultValue(null)
		.build();

	default String getStartTime() {return get(START_TIME);}

	default T setStartTime(String value) {return set(START_TIME, value);}
}
