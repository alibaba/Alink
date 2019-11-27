package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter strategy.
 * It is the fill missing value strategy, support meam, max, min or value.
 */
public interface HasStrategy<T> extends WithParams<T> {

	ParamInfo <String> STRATEGY = ParamInfoFactory
		.createParamInfo("strategy", String.class)
		.setDescription("the startegy to fill missing value, support mean, max, min or value")
		.setHasDefaultValue("mean")
		.build();

	default String getStrategy() {
		return get(STRATEGY);
	}

	default T setStrategy(String value) {
		return set(STRATEGY, value);
	}

	ParamInfo <String> FILL_VALUE = ParamInfoFactory
			.createParamInfo("fillValue", String.class)
			.setDescription("fill all missing values with fillValue")
			.setHasDefaultValue(null)
			.build();

	default String getFillValue() {
		return get(FILL_VALUE);
	}

	default T setFillValue(String value) {
		return set(FILL_VALUE, value);
	}



}
