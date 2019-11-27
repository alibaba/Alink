package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Name of the tensor column from left table.
 */
public interface HasLeftCol<T> extends WithParams<T> {
	ParamInfo <String> LEFT_COL = ParamInfoFactory
		.createParamInfo("leftCol", String.class)
		.setDescription("Name of the tensor column from left table")
		.setRequired()
		.setAlias(new String[] {"inputSelectedColName", "tensorColName1", "leftColName"})
		.build();

	default String getLeftCol() {
		return get(LEFT_COL);
	}

	default T setLeftCol(String value) {
		return set(LEFT_COL, value);
	}

}
