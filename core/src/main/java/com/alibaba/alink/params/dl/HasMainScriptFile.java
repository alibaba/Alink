package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMainScriptFile<T> extends WithParams <T> {

	ParamInfo <String> MAIN_SCRIPT_FILE = ParamInfoFactory
		.createParamInfo("mainScriptFile", String.class)
		.setDescription("path for the main script file")
		.build();

	default String getMainScriptFile() {
		return get(MAIN_SCRIPT_FILE);
	}

	default T setMainScriptFile(String value) {
		return set(MAIN_SCRIPT_FILE, value);
	}

}
