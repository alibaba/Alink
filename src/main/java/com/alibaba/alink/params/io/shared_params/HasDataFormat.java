package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDataFormat<T> extends WithParams<T> {
	// "json", "csv"
	ParamInfo <String> DATA_FORMAT = ParamInfoFactory
		.createParamInfo("dataFormat", String.class)
		.setDescription("data format")
		.setRequired()
		.setAlias(new String[] {"type"})
		.build();

	default String getDataFormat() {
		return get(DATA_FORMAT);
	}

	default T setDataFormat(String value) {
		return set(DATA_FORMAT, value);
	}
}
