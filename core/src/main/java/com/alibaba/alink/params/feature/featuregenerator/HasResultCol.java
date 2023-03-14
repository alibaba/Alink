package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasResultCol<T> extends WithParams<T> {
	ParamInfo<String> RESULT_COL = ParamInfoFactory
		.createParamInfo("resultCol", String.class)
		.setDescription("res col.")
		.setAlias(new String[]{"resCol"})
		.setRequired()
		.build();

	default String getResultCol() {
		return get(RESULT_COL);
	}

	default T setResultCol(String value) {
		return set(RESULT_COL, value);
	}
}
