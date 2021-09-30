package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPythonEnv<T> extends WithParams <T> {

	ParamInfo <String> PYTHON_ENV = ParamInfoFactory
		.createParamInfo("pythonEnv", String.class)
		.setDescription("python env")
		.setHasDefaultValue("")
		.build();

	default String getPythonEnv() {
		return get(PYTHON_ENV);
	}

	default T setPythonEnv(String value) {
		return set(PYTHON_ENV, value);
	}
}
