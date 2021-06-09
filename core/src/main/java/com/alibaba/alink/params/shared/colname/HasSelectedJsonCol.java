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
public interface HasSelectedJsonCol<T> extends WithParams <T> {
	ParamInfo <String> SELECTED_JSON_COL = ParamInfoFactory
		.createParamInfo("selectedJsonCol", String.class)
		.setDescription("Name of the selected json column used for processing")
		.setRequired()
		.build();

	default String getSelectedJsonCol() {
		return get(SELECTED_JSON_COL);
	}

	default T setSelectedJsonCol(String colName) {
		return set(SELECTED_JSON_COL, colName);
	}
}
