package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: Time interval of streaming windows, unit s.
 */
public interface HasTimeIntervalDv3<T> extends WithParams <T> {

	/**
	 * @cn 流式数据统计的时间间隔
	 * @cn-name 时间间隔
	 */
	ParamInfo <Double> TIME_INTERVAL = ParamInfoFactory
		.createParamInfo("timeInterval", Double.class)
		.setDescription("Time interval of streaming windows, unit s.")
		.setHasDefaultValue(3.0)
		.build();

	default Double getTimeInterval() {
		return get(TIME_INTERVAL);
	}

	default T setTimeInterval(Double value) {
		return set(TIME_INTERVAL, value);
	}

	default T setTimeInterval(Integer value) {
		return setTimeInterval(value.doubleValue());
	}

}
