package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter vectorColName.
 */
public interface HasVectorColDefaultAsNull<T> extends WithParams<T> {

	ParamInfo <String> VECTOR_COL = ParamInfoFactory
		.createParamInfo("vectorCol", String.class)
		.setDescription("Name of a vector column")
		.setHasDefaultValue(null)
		.setAlias(new String[] {"vectorColName", "tensorColName", "vecColName"})
		.build();

	default String getVectorCol() {
		return get(VECTOR_COL);
	}

	default T setVectorCol(String colName) {
		return set(VECTOR_COL, colName);
	}
}
