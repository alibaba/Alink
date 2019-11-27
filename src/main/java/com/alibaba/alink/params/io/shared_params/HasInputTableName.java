package com.alibaba.alink.params.io.shared_params;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasInputTableName<T> extends WithParams<T> {
	ParamInfo <String> INPUT_TABLE_NAME = ParamInfoFactory
		.createParamInfo("inputTableName", String.class)
		.setDescription("input table name")
		.setRequired()
		.setAlias(new String[] {"tableName"})
		.build();

	default String getInputTableName() {
		return get(INPUT_TABLE_NAME);
	}

	default T setInputTableName(String value) {
		return set(INPUT_TABLE_NAME, value);
	}
}
