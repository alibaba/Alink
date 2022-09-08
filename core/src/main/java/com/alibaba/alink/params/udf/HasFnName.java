package com.alibaba.alink.params.udf;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasFnName<T> extends WithParams <T> {
	ParamInfo <String> FN_NAME = ParamInfoFactory
		.createParamInfo("fnName", String.class)
		.setDescription("the built-in name of udf")
		.setRequired()
		.build();

	default T setFnName(String fnName) {
		return set(FN_NAME, fnName);
	}

	default String getFnName() {
		return get(FN_NAME);
	}
}
