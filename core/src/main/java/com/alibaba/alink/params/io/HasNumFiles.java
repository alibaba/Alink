package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNumFiles<T> extends WithParams <T> {
	/**
	 * @cn-name 文件数目
	 * @cn 文件数目
	 */
	ParamInfo <Integer> NUM_FILES = ParamInfoFactory
		.createParamInfo("numFiles", Integer.class)
		.setDescription("Number of files")
		.setHasDefaultValue(1)
		.build();

	default Integer getNumFiles() {
		return get(NUM_FILES);
	}

	default T setNumFiles(Integer value) {
		return set(NUM_FILES, value);
	}
}
