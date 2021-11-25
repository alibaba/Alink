package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTo<T> extends WithParams <T> {

	/**
	 * @cn-name 截止
	 * @cn 截止
	 */
	ParamInfo <Long> TO = ParamInfoFactory
		.createParamInfo("to", Long.class)
		.setRequired()
		.build();

	default T setTo(long value) {
		set(TO, value);
		return (T) this;
	}

	default Long getTo() {
		return get(TO);
	}
}
