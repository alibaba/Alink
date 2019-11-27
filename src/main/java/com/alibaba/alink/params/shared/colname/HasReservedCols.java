package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the names of the columns to be retained in the output table.
 */
public interface HasReservedCols<T> extends WithParams<T> {
	ParamInfo <String[]> RESERVED_COLS = ParamInfoFactory
		.createParamInfo("reservedCols", String[].class)
		.setDescription("Names of the columns to be retained in the output table")
		.setAlias(new String[] {"keepColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getReservedCols() {
		return get(RESERVED_COLS);
	}

	default T setReservedCols(String... colNames) {
		return set(RESERVED_COLS, colNames);
	}
}
