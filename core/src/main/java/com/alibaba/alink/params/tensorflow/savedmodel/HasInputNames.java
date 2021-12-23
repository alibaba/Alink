package com.alibaba.alink.params.tensorflow.savedmodel;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasInputNames<T> extends WithParams<T> {

	/**
	 * @cn-name signature中的输入名
	 * @cn signature中的输入名，多个输入时用逗号分隔
	 */
	ParamInfo <String[]> INPUT_NAMES = ParamInfoFactory
		.createParamInfo("inputNames", String[].class)
		.setDescription("input names")
		.build();

	default String[] getInputNames() {
		return get(INPUT_NAMES);
	}

	default T setInputNames(String[] value) {
		return set(INPUT_NAMES, value);
	}
}
