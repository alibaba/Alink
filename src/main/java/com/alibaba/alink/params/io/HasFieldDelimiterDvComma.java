package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasFieldDelimiterDvComma<T> extends WithParams<T> {
	ParamInfo <String> FIELD_DELIMITER = ParamInfoFactory
		.createParamInfo("fieldDelimiter", String.class)
		.setDescription("Field delimiter")
		.setHasDefaultValue(",")
		.build();

	default String getFieldDelimiter() {
		return get(FIELD_DELIMITER);
	}

	default T setFieldDelimiter(String value) {
		return set(FIELD_DELIMITER, value);
	}
}
