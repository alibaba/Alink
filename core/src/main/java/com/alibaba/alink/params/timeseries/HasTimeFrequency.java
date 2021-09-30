package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

import java.io.Serializable;

public interface HasTimeFrequency<T> extends WithParams <T> {

	ParamInfo <TimeFrequency> TIME_FREQUENCY = ParamInfoFactory
		.createParamInfo("timeFrequency", TimeFrequency.class)
		.setDescription("Frequnecy of time series.")
		.setRequired()
		.build();

	default TimeFrequency getTimeFrequency() {
		return get(TIME_FREQUENCY);
	}

	default T setTimeFrequency(TimeFrequency frequency) {
		return set(TIME_FREQUENCY, frequency);
	}

	default T setTimeFrequency(String frequency) {
		return set(TIME_FREQUENCY, ParamUtil.searchEnum(TIME_FREQUENCY, frequency));
	}

	enum TimeFrequency implements Serializable {
		EVERY_MINUTE,
		HOURLY,
		DAILY,
		WEEKLY,
		MONTHLY
	}
}
