package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the name of the table column.
 *
 * @see HasSelectedColDefaultAsNull
 * @see HasSelectedCols
 * @see HasSelectedColsDefaultAsNull
 */
public interface HasSelectedCol<T> extends WithParams<T> {

	ParamInfo <String> SELECTED_COL = ParamInfoFactory
		.createParamInfo("selectedCol", String.class)
		.setDescription("Name of the selected column used for processing")
		.setAlias(new String[] {"selectedColName"})
		.setRequired()
		.build();

	default String getSelectedCol() {
		return get(SELECTED_COL);
	}

	default T setSelectedCol(String colName) {
		return set(SELECTED_COL, colName);
	}
}
