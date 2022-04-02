package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying the name of multiple table columns.
 *
 * @see HasSelectedCol
 * @see HasSelectedColDefaultAsNull
 * @see HasSelectedColsDefaultAsNull
 */
public interface HasSelectedCols<T> extends WithParams <T> {

	@NameCn("选择的列名")
	@DescCn("计算列对应的列名列表")
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
