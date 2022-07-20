package com.alibaba.alink.params.shared.optim;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * learning rate of optimization method. The default value is null.
 */
public interface HasLearningRateDefaultAsNull<T> extends WithParams <T> {

	@NameCn("学习率")
	@DescCn("优化算法的学习率，默认0.1。")
	ParamInfo <Double> LEARNING_RATE = ParamInfoFactory
		.createParamInfo("learningRate", Double.class)
		.setDescription("learning rate of optimization method. The default value is null. Then different optimize method will set their default value by itself. ")
		.setHasDefaultValue(null)
		.build();

	default Double getLearningRate() {
		return get(LEARNING_RATE);
	}

	default T setLearningRate(Double value) {
		return set(LEARNING_RATE, value);
	}
}
