package com.alibaba.alink.params.shared.associationrules;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMinLiftDefaultAs1<T> extends WithParams <T> {
	/**
	 * @cn-name 最小提升度
	 * @cn 最小提升度，提升度是用来衡量A出现的情况下，是否会对B出现的概率有所提升。
	 */
	ParamInfo <Double> MIN_LIFT = ParamInfoFactory
		.createParamInfo("minLift", Double.class)
		.setDescription("Minimum lift")
		.setHasDefaultValue(1.0)
		.build();

	default Double getMinLift() {
		return get(MIN_LIFT);
	}

	default T setMinLift(Double value) {
		return set(MIN_LIFT, value);
	}
}
