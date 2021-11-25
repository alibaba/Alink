package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasEnableElse<T> extends WithParams <T> {
	/**
	 * @cn-name 预测时是否考虑未出现的元素
	 * @cn 预测时是否考虑未出现的元素，默认true
	 */
	ParamInfo <Boolean> ENABLE_ELSE = ParamInfoFactory
		.createParamInfo("enableElse", Boolean.class)
		.setDescription("enableElse")
		.setHasDefaultValue(Boolean.TRUE)
		.build();

	default Boolean getEnableElse() {
		return get(ENABLE_ELSE);
	}

	default T setEnableElse(Boolean value) {
		return set(ENABLE_ELSE, value);
	}
}
