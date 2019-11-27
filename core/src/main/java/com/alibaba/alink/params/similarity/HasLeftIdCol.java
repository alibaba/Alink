package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Name of the tensor column from left table.
 */
public interface HasLeftIdCol<T> extends WithParams<T> {
	ParamInfo <String> LEFT_ID_COL = ParamInfoFactory
		.createParamInfo("leftIdCol", String.class)
		.setDescription("Name of the tensor column from left table")
		.setRequired()
		.setAlias(new String[] {"inputSelectedColName", "idColName1", "leftIdColName"})
		.build();

	default String getLeftIdCol() {
		return get(LEFT_ID_COL);
	}

	default T setLeftIdCol(String value) {
		return set(LEFT_ID_COL, value);
	}
}
