package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Name of the id column from right table.
 */
public interface HasRightIdCol<T> extends WithParams<T> {

	ParamInfo <String> RIGHT_ID_COL = ParamInfoFactory
		.createParamInfo("rightIdCol", String.class)
		.setDescription("Name of the id column from right table")
		.setRequired()
		.setAlias(new String[] {"inputSelectedColName", "idColName2", "rightIdColName"})
		.build();

	default String getRightIdCol() {
		return get(RIGHT_ID_COL);
	}

	default T setRightIdCol(String value) {
		return set(RIGHT_ID_COL, value);
	}
}
