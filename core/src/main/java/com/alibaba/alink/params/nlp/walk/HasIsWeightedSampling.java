package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIsWeightedSampling<T> extends WithParams <T> {
	ParamInfo <Boolean> IS_WEIGHTED_SAMPLING = ParamInfoFactory
		.createParamInfo("isWeightedSampling", Boolean.class)
		.setDescription("is weighted sampling")
		.setHasDefaultValue(true)
		.build();

	default Boolean getIsWeightedSampling() {return get(IS_WEIGHTED_SAMPLING);}

	default T setIsWeightedSampling(Boolean value) {return set(IS_WEIGHTED_SAMPLING, value);}
}
