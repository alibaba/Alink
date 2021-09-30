package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLengthCol<T> extends WithParams <T> {

	ParamInfo <String> LENGTH_COL = ParamInfoFactory
		.createParamInfo("lengthCol", String.class)
		.setDescription("Name of the length column")
		.setHasDefaultValue("length")
		.build();

	default String getLengthCol() {
		return get(LENGTH_COL);
	}

	default T setLengthCol(String colName) {
		return set(LENGTH_COL, colName);
	}
}
