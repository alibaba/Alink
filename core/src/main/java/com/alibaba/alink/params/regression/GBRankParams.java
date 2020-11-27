package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface GBRankParams<T> extends
	LambdaMartNdcgParams <T> {

	ParamInfo <Double> TAU = ParamInfoFactory
		.createParamInfo("tau", Double.class)
		.setHasDefaultValue(0.6)
		.build();

	ParamInfo <Double> P = ParamInfoFactory
		.createParamInfo("p", Double.class)
		.setHasDefaultValue(1.0)
		.build();

	default T setTau(Double value) {
		return set(TAU, value);
	}

	default Double getTau() {
		return get(TAU);
	}

	default T setP(Double value) {
		return set(P, value);
	}

	default Double getP() {
		return get(P);
	}
}

