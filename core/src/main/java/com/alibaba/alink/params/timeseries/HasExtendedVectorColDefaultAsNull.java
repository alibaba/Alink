package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasExtendedVectorColDefaultAsNull<T> extends WithParams <T> {

	ParamInfo <String> EXTENDED_VECTOR_COL = ParamInfoFactory
		.createParamInfo("extendedVectorCol", String.class)
		.setDescription("Names of the columns used for processing")
		.setHasDefaultValue(null)
		.build();

	default String getExtendedVectorCol() {
		return get(EXTENDED_VECTOR_COL);
	}

	default T setExtendedVectorCol(String colName) {
		return set(EXTENDED_VECTOR_COL, colName);
	}
}
