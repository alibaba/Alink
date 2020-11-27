package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDefaultDatabase<T> extends WithParams <T> {
	ParamInfo <String> DEFAULT_DATABASE = ParamInfoFactory
		.createParamInfo("defaultDatabase", String.class)
		.setDescription("the default database")
		.setHasDefaultValue(null)
		.build();

	default String getDefaultDatabase() {
		return get(DEFAULT_DATABASE);
	}

	default T setDefaultDatabase(String value) {
		return set(DEFAULT_DATABASE, value);
	}
}
