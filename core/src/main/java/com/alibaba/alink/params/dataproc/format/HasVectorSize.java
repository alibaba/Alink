package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Trait for parameter vectorSize.
 */
public interface HasVectorSize<T> extends WithParams <T> {

	/**
	 * @cn-name 向量长度
	 * @cn 向量长度
	 */
	ParamInfo <Long> VECTOR_SIZE = ParamInfoFactory
		.createParamInfo("vectorSize", Long.class)
		.setDescription("Size of the vector")
		.setHasDefaultValue(-1L)
		.build();

	default Long getVectorSize() {
		return get(VECTOR_SIZE);
	}

	default T setVectorSize(Long size) {
		return set(VECTOR_SIZE, size);
	}

	default T setVectorSize(Integer size) {
		return set(VECTOR_SIZE, size.longValue());
	}
}
