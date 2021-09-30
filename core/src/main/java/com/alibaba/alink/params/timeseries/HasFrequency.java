package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasFrequency<T> extends WithParams <T> {
	ParamInfo <Integer> FREQUENCY = ParamInfoFactory
		.createParamInfo("frequency", Integer.class)
		.setDescription("Defines the number of observations in a single period," +
			" and used during seasonal decomposition.")
		.setHasDefaultValue(10)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getFrequency() {
		return get(FREQUENCY);
	}

	default T setFrequency(Integer value) {
		return set(FREQUENCY, value);
	}
}
