package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasIsWeightedSampling<T> extends WithParams <T> {

	/**
	 * @cn-name 是否为加权采样
	 * @cn 该算法支持加权采样和随机采样两种采样方式
	 */
	ParamInfo <Boolean> IS_WEIGHTED_SAMPLING = ParamInfoFactory
		.createParamInfo("isWeightedSampling", Boolean.class)
		.setDescription("is weighted sampling")
		.setHasDefaultValue(true)
		.build();

	default Boolean getIsWeightedSampling() {return get(IS_WEIGHTED_SAMPLING);}

	default T setIsWeightedSampling(Boolean value) {return set(IS_WEIGHTED_SAMPLING, value);}
}
