package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * learning rate of adagrad.
 */
public interface HasLearnRateDefaultAs001<T> extends WithParams <T> {
	/**
	 * @cn-name 学习率
	 * @cn 学习率
	 */
	ParamInfo <Double> LEARN_RATE = ParamInfoFactory
			.createParamInfo("learnRate", Double.class)
			.setDescription("learn rate")
			.setHasDefaultValue(0.01)
			.build();

	default Double getLearnRate() {
		return get(LEARN_RATE);
	}

	default T setLearnRate(Double value) {
		return set(LEARN_RATE, value);
	}

}
