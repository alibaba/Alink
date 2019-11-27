package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinLiftDefaultAs1<T> extends WithParams<T> {
	ParamInfo <Double> MIN_LIFT = ParamInfoFactory
		.createParamInfo("minLift", Double.class)
		.setDescription("Minimum lift")
		.setHasDefaultValue(1.0)
		.build();

	default Double getMinLift() {
		return get(MIN_LIFT);
	}

	default T setMinLift(Double value) {
		return set(MIN_LIFT, value);
	}
}
