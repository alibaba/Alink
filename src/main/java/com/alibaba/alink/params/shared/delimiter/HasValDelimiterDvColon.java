package com.alibaba.alink.params.shared.delimiter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasValDelimiterDvColon<T> extends WithParams<T> {
	ParamInfo <String> VAL_DELIMITER = ParamInfoFactory
		.createParamInfo("valDelimiter", String.class)
		.setDescription("Delimiter used between keys and values when data in the input table is in sparse format")
		.setHasDefaultValue(":")
		.build();

	default String getValDelimiter() {
		return get(VAL_DELIMITER);
	}

	default T setValDelimiter(String value) {
		return set(VAL_DELIMITER, value);
	}
}
