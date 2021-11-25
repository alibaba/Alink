package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

/**
 * Param of the smoothing factor.
 */
public interface HasSmoothing<T> extends WithParams <T> {

	/**
	 * @cn-name 算法参数
	 * @cn 光滑因子，默认为1.0
	 */
	ParamInfo <Double> SMOOTHING = ParamInfoFactory
		.createParamInfo("smoothing", Double.class)
		.setDescription("the smoothing factor")
		.setHasDefaultValue(1.0)
		.setValidator(new MinValidator <>(0.0))
		.build();

	default Double getSmoothing() {
		return get(SMOOTHING);
	}

	default T setSmoothing(Double value) {
		return set(SMOOTHING, value);
	}
}
