package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * second order item regular number.
 */
public interface HasLambda2DefaultAs0<T> extends WithParams <T> {
	/**
	 * @cn-name 二次项正则化系数
	 * @cn 二次项正则化系数
	 */
	ParamInfo <Double> LAMBDA_2 = ParamInfoFactory
			.createParamInfo("lambda2", Double.class)
			.setDescription("lambda_2")
			.setHasDefaultValue(0.0)
			.build();

	default Double getLambda2() {
		return get(LAMBDA_2);
	}

	default T setLambda2(Double value) {
		return set(LAMBDA_2, value);
	}
}
