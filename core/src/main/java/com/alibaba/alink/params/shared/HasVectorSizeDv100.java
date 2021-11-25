package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasVectorSizeDv100<T> extends WithParams <T> {
	/**
	 * @cn-name embedding的向量长度
	 * @cn embedding的向量长度
	 */
	ParamInfo <Integer> VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("vectorSize", Integer.class)
		.setDescription("vector size of embedding")
		.setHasDefaultValue(100)
		.setValidator(new MinValidator <>(1))
		.setAlias(new String[] {"dim"})
		.build();

	default Integer getVectorSize() {
		return get(VECTOR_SIZE);
	}

	default T setVectorSize(Integer value) {
		return set(VECTOR_SIZE, value);
	}
}
