package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasHorizon<T> extends WithParams <T> {

	ParamInfo <Integer> HORIZON = ParamInfoFactory
		.createParamInfo("horizon", Integer.class)
		.setDescription("horizon")
		.setHasDefaultValue(12)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getHorizon() {
		return get(HORIZON);
	}

	default T setHorizon(Integer value) {
		return set(HORIZON, value);
	}
}
