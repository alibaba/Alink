package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasNegative<T> extends WithParams <T> {
	ParamInfo <Integer> NEGATIVE = ParamInfoFactory
		.createParamInfo("negative", Integer.class)
		.setDescription("The negative sampling size")
		.setHasDefaultValue(5)
		.build();

	default Integer getNegative() {
		return get(NEGATIVE);
	}

	default T setNegative(Integer value) {
		return set(NEGATIVE, value);
	}
}
