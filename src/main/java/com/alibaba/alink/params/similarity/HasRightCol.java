package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Name of the tensor column from the right table.
 */
public interface HasRightCol<T> extends WithParams<T> {
	ParamInfo <String> RIGHT_COL = ParamInfoFactory
		.createParamInfo("rightCol", String.class)
		.setDescription("Name of the tensor column from the right table")
		.setRequired()
		.setAlias(new String[] {"mapSelectedColName", "tensorColName2", "rightColName"})
		.build();

	default String getRightCol() {
		return get(RIGHT_COL);
	}

	default T setRightCol(String value) {
		return set(RIGHT_COL, value);
	}

}
