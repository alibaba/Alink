package com.alibaba.alink.params.recommendation.fm;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * learning rate of adagrad.
 */
public interface HasLearnRateDefaultAs001<T> extends WithParams <T> {
	@NameCn("学习率")
	@DescCn("学习率")
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
