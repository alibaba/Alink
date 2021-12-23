package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasUserCol<T> extends WithParams <T> {
	/**
	 * @cn-name User列列名
	 * @cn User列列名
	 */
	ParamInfo <String> USER_COL = ParamInfoFactory
		.createParamInfo("userCol", String.class)
		.setAlias(new String[] {"userColName"})
		.setDescription("User column name")
		.setRequired()
		.build();

	default String getUserCol() {
		return get(USER_COL);
	}

	default T setUserCol(String value) {
		return set(USER_COL, value);
	}
}
