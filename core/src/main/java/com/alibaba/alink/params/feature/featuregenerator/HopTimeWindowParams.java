package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface HopTimeWindowParams<T> extends
	GroupTimeWindowParams <T>,
	HasWindowTime <T> {

	/**
	 * @cn-name 滑动窗口大小
	 * @cn 滑动窗口大小
	 */
	ParamInfo<Double> HOP_TIME = ParamInfoFactory
		.createParamInfo("hopTime", Double.class)
		.setDescription("hop time interval")
		.setRequired()
		.build();

	default Double getHopTime() {return get(HOP_TIME);}

	default T setHopTime(Double value) {return set(HOP_TIME, value);}

	default T setHopTime(Integer value) {return set(HOP_TIME, (double)value);}
}
