package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasAlpha<T> extends WithParams<T> {
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("alpha")
		.setHasDefaultValue(0.1)
		.build();

	default Double getAlpha() {return get(ALPHA);}

	default T setAlpha(Double value) {return set(ALPHA, value);}
}
