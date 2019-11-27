package com.alibaba.alink.params.shared.delimiter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasColDelimiterDvComma<T> extends WithParams<T> {
	ParamInfo <String> COL_DELIMITER = ParamInfoFactory
		.createParamInfo("colDelimiter", String.class)
		.setDescription("Delimiter used between key-value pairs when data in the input table is in sparse format")
		.setHasDefaultValue(",")
		.build();

	default String getColDelimiter() {
		return get(COL_DELIMITER);
	}

	default T setColDelimiter(String value) {
		return set(COL_DELIMITER, value);
	}
}
