package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying name of the output column with a null default value.
 *
 * @see HasOutputCol
 * @see HasOutputCols
 * @see HasOutputColsDefaultAsNull
 */
public interface HasOutputColDefaultAsNull<T> extends WithParams<T> {

	ParamInfo <String> OUTPUT_COL = ParamInfoFactory
		.createParamInfo("outputCol", String.class)
		.setDescription("Name of the output column")
		.setAlias(new String[] {"outputColName"})
		.setHasDefaultValue(null)
		.build();

	default String getOutputCol() {
		return get(OUTPUT_COL);
	}

	default T setOutputCol(String colName) {
		return set(OUTPUT_COL, colName);
	}
}
