package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasFilePath<T> extends WithParams<T> {
	ParamInfo <String> FILE_PATH = ParamInfoFactory
		.createParamInfo("filePath", String.class)
		.setDescription("File path")
		.setRequired()
		.build();

	default String getFilePath() {
		return get(FILE_PATH);
	}

	default T setFilePath(String value) {
		return set(FILE_PATH, value);
	}
}
