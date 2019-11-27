package com.alibaba.alink.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPredMethod<T> extends WithParams<T> {
	ParamInfo <String> PRED_METHOD = ParamInfoFactory
		.createParamInfo("predMethod", String.class)
		.setDescription("Method to predict doc vector, support 3 method: avg, min and max, default value is avg.")
		.setHasDefaultValue("avg")
		.setAlias(new String[] {"generationType", "algorithmType"})
		.build();

	default String getPredMethod() {
		return get(PRED_METHOD);
	}

	default T setPredMethod(String value) {
		return set(PRED_METHOD, value);
	}
}
