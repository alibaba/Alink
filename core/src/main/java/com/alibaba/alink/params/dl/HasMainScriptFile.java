package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasMainScriptFile<T> extends WithParams <T> {
	/**
	 * @cn 主脚本文件路径，需要是参数 userFiles 中的一项，并且包含 main 函数
	 * @cn-name 主脚本文件路径
	 */
	ParamInfo <String> MAIN_SCRIPT_FILE = ParamInfoFactory
		.createParamInfo("mainScriptFile", String.class)
		.setDescription("Path for the main script file, containing a function named main, one entry of parameter userFiles")
		.setRequired()
		.build();

	default String getMainScriptFile() {
		return get(MAIN_SCRIPT_FILE);
	}

	default T setMainScriptFile(String value) {
		return set(MAIN_SCRIPT_FILE, value);
	}

}
