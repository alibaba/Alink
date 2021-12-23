package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasOutputTableName<T> extends WithParams <T> {
	/**
	 * @cn-name 输出表名字
	 * @cn 输出表名字
	 */
	ParamInfo <String> INPUT_TABLE_NAME = ParamInfoFactory
		.createParamInfo("outputTableName", String.class)
		.setDescription("output table name")
		.setRequired()
		.setAlias(new String[] {"tableName"})
		.build();

	default String getOutputTableName() {
		return get(INPUT_TABLE_NAME);
	}

	default T setOutputTableName(String value) {
		return set(INPUT_TABLE_NAME, value);
	}
}
