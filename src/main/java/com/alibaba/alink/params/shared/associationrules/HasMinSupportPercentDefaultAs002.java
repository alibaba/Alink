package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinSupportPercentDefaultAs002<T> extends WithParams<T> {
	ParamInfo <Double> MIN_SUPPORT_PERCENT = ParamInfoFactory
		.createParamInfo("minSupportPercent", Double.class)
		.setDescription("Minimum support percent")
		.setHasDefaultValue(0.02)
		.build();

	default Double getMinSupportPercent() {
		return get(MIN_SUPPORT_PERCENT);
	}

	default T setMinSupportPercent(Double value) {
		return set(MIN_SUPPORT_PERCENT, value);
	}
}
