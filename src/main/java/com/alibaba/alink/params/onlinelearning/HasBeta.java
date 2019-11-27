package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasBeta<T> extends WithParams<T> {
	ParamInfo <Double> BETA = ParamInfoFactory
		.createParamInfo("beta", Double.class)
		.setDescription("beta")
		.setHasDefaultValue(1.0)
		.build();

	default Double getBeta() {return get(BETA);}

	default T setBeta(Double value) {return set(BETA, value);}
}
