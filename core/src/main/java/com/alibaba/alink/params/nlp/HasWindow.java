package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasWindow<T> extends WithParams <T> {
	/**
	 * @cn-name 窗口大小
	 * @cn 窗口大小
	 */
	ParamInfo <Integer> WINDOW = ParamInfoFactory
		.createParamInfo("window", Integer.class)
		.setDescription("the length of window in w2v")
		.setHasDefaultValue(5)
		.build();

	default Integer getWindow() {
		return get(WINDOW);
	}

	default T setWindow(Integer value) {
		return set(WINDOW, value);
	}
}
