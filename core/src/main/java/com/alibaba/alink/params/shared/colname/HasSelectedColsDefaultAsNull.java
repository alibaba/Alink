package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the name of multiple table columns with null default value.
 *
 * @see HasSelectedCol
 * @see HasSelectedColDefaultAsNull
 * @see HasSelectedCols
 */
public interface HasSelectedColsDefaultAsNull<T> extends WithParams <T> {

	/**
	 * @cn-name 选中的列名数组
	 * @cn 计算列对应的列名列表
	 */
	ParamInfo <String[]> SELECTED_COLS = ParamInfoFactory
		.createParamInfo("selectedCols", String[].class)
		.setDescription("Names of the columns used for processing")
		.setAlias(new String[] {"selectedColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getSelectedCols() {
		return get(SELECTED_COLS);
	}

	default T setSelectedCols(String... colNames) {
		return set(SELECTED_COLS, colNames);
	}
}
