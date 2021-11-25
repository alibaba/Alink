package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface AlsImplicitTrainParams<T> extends
	AlsTrainParams <T> {

	/**
	 * @cn-name 隐式偏好模型系数alpha
	 * @cn 隐式偏好模型系数alpha
	 */
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("The alpha in implicit preference model.")
		.setHasDefaultValue(40.0)
		.build();

	default Double getAlpha() {
		return get(ALPHA);
	}

	default T setAlpha(Double value) {
		return set(ALPHA, value);
	}
}
