package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying names of multiple output columns. The default parameter value is
 * null.
 *
 * @see HasOutputCol
 * @see HasOutputColDefaultAsNull
 * @see HasOutputCols
 */
public interface HasOutputColsDefaultAsNull<T> extends WithParams<T> {
	ParamInfo <String[]> OUTPUT_COLS = ParamInfoFactory
		.createParamInfo("outputCols", String[].class)
		.setDescription("Names of the output columns")
		.setAlias(new String[] {"outputColNames"})
		.setHasDefaultValue(null)
		.build();

	default String[] getOutputCols() {
		return get(OUTPUT_COLS);
	}

	default T setOutputCols(String... colNames) {
		return set(OUTPUT_COLS, colNames);
	}
}
