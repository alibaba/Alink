package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasDbName<T> extends WithParams <T> {
	/**
	 * @cn-name 数据库名字
	 * @cn 数据库名字
	 */
	ParamInfo <String> DB_NAME = ParamInfoFactory
		.createParamInfo("dbName", String.class)
		.setDescription("db name")
		.setRequired()
		.build();

	default String getDbName() {return get(DB_NAME);}

	default T setDbName(String value) {return set(DB_NAME, value);}
}
