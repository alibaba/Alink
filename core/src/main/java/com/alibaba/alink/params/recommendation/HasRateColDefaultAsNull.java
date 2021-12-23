package com.alibaba.alink.params.recommendation;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasRateColDefaultAsNull<T> extends WithParams <T> {
	/**
	 * @cn-name 打分列列名
	 * @cn 打分列列名
	 */
	ParamInfo <String> RATE_COL = ParamInfoFactory
		.createParamInfo("rateCol", String.class)
		.setAlias(new String[] {"rateColName"})
		.setDescription("Rating column name")
		.setHasDefaultValue(null)
		.build();

	default String getRateCol() {
		return get(RATE_COL);
	}

	default T setRateCol(String value) {
		return set(RATE_COL, value);
	}
}
