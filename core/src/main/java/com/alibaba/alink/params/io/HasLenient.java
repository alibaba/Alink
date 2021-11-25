package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLenient<T> extends WithParams <T> {

	/**
	 * @cn-name 是否容错
	 * @cn 若为true，当解析失败时丢弃该数据；若为false，解析失败是抛异常
	 */
	ParamInfo <Boolean> LENIENT = ParamInfoFactory
		.createParamInfo("lenient", Boolean.class)
		.setDescription("lenient")
		.setHasDefaultValue(false)
		.build();

	default Boolean getLenient() {
		return get(LENIENT);
	}

	default T setLenient(Boolean value) {
		return set(LENIENT, value);
	}
}
