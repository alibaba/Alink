package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasP<T> extends WithParams <T> {
	ParamInfo <Double> P = ParamInfoFactory
		.createParamInfo("p", Double.class)
		.setDescription(
			"Return parameter, p. Parameter p controls the likelihood of immediately revisiting a node in the walk.")
		.setHasDefaultValue(1.0)
		.build();

	default Double getP() {
		return get(P);
	}

	default T setP(double value) {
		return set(P, value);
	}
}
