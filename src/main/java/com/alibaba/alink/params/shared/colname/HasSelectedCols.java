package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the name of multiple table columns.
 *
 * @see HasSelectedCol
 * @see HasSelectedColDefaultAsNull
 * @see HasSelectedColsDefaultAsNull
 */
public interface HasSelectedCols<T> extends WithParams<T> {

	ParamInfo <String[]> SELECTED_COLS = ParamInfoFactory
		.createParamInfo("selectedCols", String[].class)
		.setDescription("Names of the columns used for processing")
		.setAlias(new String[] {"selectedColNames"})
		.setRequired()
		.build();

	default String[] getSelectedCols() {
		return get(SELECTED_COLS);
	}

	default T setSelectedCols(String... colNames) {
		return set(SELECTED_COLS, colNames);
	}
}
