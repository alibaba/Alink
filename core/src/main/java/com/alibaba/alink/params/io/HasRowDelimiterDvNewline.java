package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRowDelimiterDvNewline<T> extends WithParams<T> {
	ParamInfo <String> ROW_DELIMITER = ParamInfoFactory
		.createParamInfo("rowDelimiter", String.class)
		.setDescription("Row delimiter")
		.setHasDefaultValue("\n")
		.build();

	default String getRowDelimiter() {
		return get(ROW_DELIMITER);
	}

	default T setRowDelimiter(String value) {
		return set(ROW_DELIMITER, value);
	}
}
