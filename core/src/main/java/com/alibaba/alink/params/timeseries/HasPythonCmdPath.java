package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPythonCmdPath<T> extends WithParams <T> {

	ParamInfo <String> PYTHON_CMD_PATH = ParamInfoFactory
		.createParamInfo("pythonCmdPath", String.class)
		.setDescription("python cmd path")
		.setHasDefaultValue(null)
		.build();

	default String getPythonCmdPath() {
		return get(PYTHON_CMD_PATH);
	}

	default T setPythonCmdPath(String value) {
		return set(PYTHON_CMD_PATH, value);
	}
}
