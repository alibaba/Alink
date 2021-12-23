package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface ModelStreamFileSinkParams<T> extends WithParams <T>,
	HasFilePath <T> {

	/**
	 * @cn-name 保存模型的数目
	 * @cn 实时写出模型的数目上限
	 */
	ParamInfo <Integer> NUM_KEEP_MODEL = ParamInfoFactory
		.createParamInfo("numKeepModel", Integer.class)
		.setDescription("num of keep model.")
		.setHasDefaultValue(Integer.MAX_VALUE)
		.build();

	default int getNumKeepModel() {
		return get(NUM_KEEP_MODEL);
	}

	default T setNumKeepModel(int value) {
		return set(NUM_KEEP_MODEL, value);
	}
}
