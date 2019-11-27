package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * IO name.
 */
public interface HasIoName<T> extends WithParams<T> {
	ParamInfo <String> IO_NAME = ParamInfoFactory
		.createParamInfo("ioName", String.class)
		.setDescription("io name")
		.setRequired()
		.build();

	default String getRowDelimiter() {
		return get(IO_NAME);
	}

	default T setRowDelimiter(String value) {
		return set(IO_NAME, value);
	}
}

