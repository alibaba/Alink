package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRandomWindow<T> extends WithParams<T> {
	ParamInfo <String> RANDOM_WINDOW = ParamInfoFactory
		.createParamInfo("randomWindow", String.class)
		.setDescription("Is random window or not")
		.setHasDefaultValue("true")
		.build();

	default String getRandomWindow() {
		return get(RANDOM_WINDOW);
	}

	default T setRandomWindow(String value) {
		return set(RANDOM_WINDOW, value);
	}
}
