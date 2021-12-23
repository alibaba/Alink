package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the name of the table column.
 */
public interface HasJsonCol<T> extends WithParams <T> {

	/**
	 * @cn-name JSON列名
	 * @cn JSON列的列名
	 */
	ParamInfo <String> JSON_COL = ParamInfoFactory
		.createParamInfo("jsonCol", String.class)
		.setDescription("Name of the CSV column")
		.setRequired()
		.build();

	default String getJsonCol() {
		return get(JSON_COL);
	}

	default T setJsonCol(String colName) {
		return set(JSON_COL, colName);
	}
}
