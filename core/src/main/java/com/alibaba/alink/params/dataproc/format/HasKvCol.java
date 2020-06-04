package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the name of the table column.
 */
public interface HasKvCol<T> extends WithParams <T> {

	/**
	 * @cn-name KV列名
	 * @cn KV列的列名
	 */
	ParamInfo <String> KV_COL = ParamInfoFactory
		.createParamInfo("kvCol", String.class)
		.setDescription("Name of the KV column")
		.setRequired()
		.setAlias(new String[]{"selectedCol", "selectedColName"})
		.build();

	default String getKvCol() {
		return get(KV_COL);
	}

	default T setKvCol(String colName) {
		return set(KV_COL, colName);
	}
}
