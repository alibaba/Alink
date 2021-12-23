package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasStride<T> extends WithParams <T> {
	/**
	 * @cn-name horizon大小
	 * @cn horizon大小
	 */
	ParamInfo <Integer> STRIDE = ParamInfoFactory
		.createParamInfo("stride", Integer.class)
		.setDescription("stride")
		.setHasDefaultValue(12)
		.setValidator(new MinValidator <>(1))
		.build();

	default Integer getStride() {
		return get(STRIDE);
	}

	default T setStride(Integer value) {
		return set(STRIDE, value);
	}
}
