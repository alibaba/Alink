package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface DerbyDBParams<T> extends WithParams<T> {

	/**
	 * Param "dbName"
	 */
	ParamInfo <String> DB_NAME = ParamInfoFactory
		.createParamInfo("dbName", String.class)
		.setDescription("db name")
		.setRequired()
		.build();
	/**
	 * Param "password"
	 */
	ParamInfo <String> PASSWORD = ParamInfoFactory
		.createParamInfo("password", String.class)
		.setDescription("password")
		.setHasDefaultValue(null)
		.build();
	/**
	 * Param "username"
	 */
	ParamInfo <String> USERNAME = ParamInfoFactory
		.createParamInfo("username", String.class)
		.setDescription("username")
		.setHasDefaultValue(null)
		.build();

	default String getDbName() {
		return get(DB_NAME);
	}

	default T setDbName(String value) {
		return set(DB_NAME, value);
	}

	default String getPassword() {
		return get(PASSWORD);
	}

	default T setPassword(String value) {
		return set(PASSWORD, value);
	}

	default String getUsername() {
		return get(USERNAME);
	}

	default T setUsername(String value) {
		return set(USERNAME, value);
	}
}
