package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param of the name of the label column in the input table.
 * @param <T>
 */
public interface HasLabelCol<T> extends WithParams<T> {
	ParamInfo <String> LABEL_COL = ParamInfoFactory
		.createParamInfo("labelCol", String.class)
		.setDescription("Name of the label column in the input table")
		.setAlias(new String[] {"labelColName"})
		.setRequired()
		.build();

	default String getLabelCol() {
		return get(LABEL_COL);
	}

	default T setLabelCol(String colName) {
		return set(LABEL_COL, colName);
	}
}
