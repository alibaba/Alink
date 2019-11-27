package com.alibaba.alink.params.shared.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRateCol<T> extends WithParams<T> {
	ParamInfo <String> RATE_COL = ParamInfoFactory
		.createParamInfo("rateCol", String.class)
		.setAlias(new String[]{"rateColName"})
		.setDescription("Rating column name")
		.setRequired()
		.build();

	default String getRateCol() {
		return get(RATE_COL);
	}

	default T setRateCol(String value) {
		return set(RATE_COL, value);
	}
}
