package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * linear item regular number.
 */
public interface HasLambda1DefaultAs0<T> extends WithParams <T> {
	/**
	 * @cn-name 线性项正则化系数
	 * @cn 线性项正则化系数
	 */
	ParamInfo <Double> LAMBDA_1 = ParamInfoFactory
			.createParamInfo("lambda1", Double.class)
			.setDescription("lambda1")
			.setHasDefaultValue(0.0)
			.build();

	default Double getLambda1() {
		return get(LAMBDA_1);
	}

	default T setLambda1(Double value) {
		return set(LAMBDA_1, value);
	}
}
