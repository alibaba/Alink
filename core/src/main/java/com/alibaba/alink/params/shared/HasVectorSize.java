package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasVectorSize<T> extends WithParams <T> {

	/**
	 * @cn-name 向量长度
	 * @cn 向量的长度
	 */
	ParamInfo <Integer> VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("vectorSize", Integer.class)
		.setDescription("vector size of embedding")
		.setRequired()
		.setAlias(new String[] {"vectorSize", "inputDim"})
		.build();

	default Integer getVectorSize() {
		return get(VECTOR_SIZE);
	}

	default T setVectorSize(Integer value) {
		return set(VECTOR_SIZE, value);
	}
}
