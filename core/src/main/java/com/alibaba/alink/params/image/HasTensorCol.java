package com.alibaba.alink.params.image;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTensorCol<T> extends WithParams <T> {

	ParamInfo <String> TENSOR_COL = ParamInfoFactory
		.createParamInfo("tensorCol", String.class)
		.setDescription("tensor column")
		.setRequired()
		.build();

	default String getTensorCol() {
		return get(TENSOR_COL);
	}

	default T setTensorCol(String value) {
		return set(TENSOR_COL, value);
	}
}
