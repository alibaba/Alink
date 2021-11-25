package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter selectedColName.
 */
public interface HasSelectedColDefaultAsContent<T> extends WithParams <T> {

	/**
	 * @cn 计算列对应的列名
	 * @cn-name 计算列对应的列名
	 */
	ParamInfo <String> SELECTED_COL = ParamInfoFactory
		.createParamInfo("selectedCol", String.class)
		.setDescription("Name of the selected column used for processing")
		.setRequired()
		.setAlias(new String[] {"selectedColName", "contentColName"})
		.build();

	default String getSelectedCol() {
		return get(SELECTED_COL);
	}

	default T setSelectedCol(String colName) {
		return set(SELECTED_COL, colName);
	}
}
