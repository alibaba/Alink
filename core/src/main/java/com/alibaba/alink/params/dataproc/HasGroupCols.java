package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasGroupCols<T> extends WithParams <T> {
	/**
	 * @cn-name 分组列列名数组
	 * @cn  分组列列名数组
	 */
	ParamInfo <String[]> GROUP_COLS = ParamInfoFactory
		.createParamInfo("groupCols", String[].class)
		.setDescription("group column names")
		.setRequired()
		.build();

	default String[] getGroupCols() {
		return get(GROUP_COLS);
	}

	default T setGroupCols(String... value) {
		return set(GROUP_COLS, value);
	}
}
