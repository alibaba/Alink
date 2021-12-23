package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDiscreteThresholds<T> extends WithParams <T> {
	/**
	 * @cn-name 离散个数阈值
	 * @cn 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。
	 */
	ParamInfo <Integer> DISCRETE_THRESHOLDS = ParamInfoFactory
		.createParamInfo("discreteThresholds", Integer.class)
		.setDescription("discreteThreshold")
		.setAlias(new String[] {"discreteThreshold"})
		.setHasDefaultValue(Integer.MIN_VALUE)
		.build();

	default Integer getDiscreteThresholds() {
		return get(DISCRETE_THRESHOLDS);
	}

	default T setDiscreteThresholds(Integer value) {
		return set(DISCRETE_THRESHOLDS, value);
	}

}
