package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasWindowSize_2<T> extends WithParams <T> {
	ParamInfo <Integer> WINDOW_SIZE = ParamInfoFactory
		.createParamInfo("windowSize", Integer.class)
		.setDescription("window size")
		.setHasDefaultValue(2)
		.setValidator(new MinValidator <>(1))
		.setAlias(new String[] {"k", "n"})
		.build();

	default Integer getWindowSize() {
		return get(WINDOW_SIZE);
	}

	default T setWindowSize(Integer value) {
		return set(WINDOW_SIZE, value);
	}
}
