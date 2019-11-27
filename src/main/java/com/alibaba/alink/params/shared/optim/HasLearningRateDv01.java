package com.alibaba.alink.params.shared.optim;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * learning rate of optimization method. The default value is 0.1.
 *
 */
public interface HasLearningRateDv01<T> extends WithParams<T> {

	ParamInfo <Double> LEARNING_RATE = ParamInfoFactory
		.createParamInfo("learningRate", Double.class)
		.setDescription("learning rate of optimization method. The default value is 0.1")
		.setHasDefaultValue(0.1)
		.build();

	default Double getLearningRate() {
		return get(LEARNING_RATE);
	}

	default T setLearningRate(Double value) {
		return set(LEARNING_RATE, value);
	}
}
