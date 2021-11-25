package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasFrom<T> extends WithParams <T> {

	/**
	 * @cn-name 开始
	 * @cn 开始
	 */
	ParamInfo <Long> FROM = ParamInfoFactory
		.createParamInfo("from", Long.class)
		.setRequired()
		.build();

	default T setFrom(long value) {
		set(FROM, value);
		return (T) this;
	}

	default Long getFrom() {
		return get(FROM);
	}
}
