package com.alibaba.alink.params.dataproc.tensor;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

public interface TensorReshapeParams<T> extends SISOMapperParams <T> {

	/**
	 * Param "size"
	 */
	ParamInfo <Integer[]> SIZE = ParamInfoFactory
		.createParamInfo("size", Integer[].class)
		.setDescription("size")
		.setRequired()
		.build();

	default Integer[] getSize() {
		return get(SIZE);
	}

	default T setSize(Integer[] value) {
		return set(SIZE, value);
	}

}
