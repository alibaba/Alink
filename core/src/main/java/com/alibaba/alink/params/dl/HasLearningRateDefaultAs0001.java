package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLearningRateDefaultAs0001<T> extends WithParams <T> {
	@NameCn("学习率")
	@DescCn("学习率")
	ParamInfo <Double> LEARNING_RATE = ParamInfoFactory
		.createParamInfo("learningRate", Double.class)
		.setDescription("learn rate")
		.setHasDefaultValue(0.001)
		.build();

	default Double getLearningRate() {
		return get(LEARNING_RATE);
	}

	default T setLearningRate(Double value) {
		return set(LEARNING_RATE, value);
	}
}
